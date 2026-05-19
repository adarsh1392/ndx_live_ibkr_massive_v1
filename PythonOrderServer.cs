// PythonOrderServer.cs  —  NinjaTrader 8  STRATEGY
//
// ── INSTALL ────────────────────────────────────────────────────────────────
//  1. Copy this file to:
//       Documents\NinjaTrader 8\bin\Custom\Strategies\PythonOrderServer.cs
//
//  2. Open NinjaScript Editor in NinjaTrader:
//       New menu → NinjaScript Editor
//     Then press F5 to compile. Should say "Compiled successfully."
//
//  3. Open a chart of MNQ (any timeframe — even 1 min)
//     Right-click the chart → Strategies → Add Strategy → PythonOrderServer
//     Set:  Account = your Sim account,  Port = 5557
//     Click OK / Enable
//
//  4. You will see in the Output tab:
//       [PythonOrderServer] Listening on 127.0.0.1:5557
//
// ── COMMANDS ───────────────────────────────────────────────────────────────
//    ENTRY|LONG|1     → BUY    1 contract at market
//    ENTRY|SHORT|1    → SELL SHORT 1 contract at market
//    EXIT|LONG|1      → SELL   1 contract at market  (close long)
//    EXIT|SHORT|1     → BUY TO COVER 1 contract
//    FLATTEN          → flatten entire position
//    PING             → non-destructive connectivity check
//
// ── RESPONSE ───────────────────────────────────────────────────────────────
//    "OK: ..."   /   "ERROR: ..."
// ───────────────────────────────────────────────────────────────────────────

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;

namespace NinjaTrader.NinjaScript.Strategies
{
    public class PythonOrderServer : Strategy
    {
        private TcpListener   _listener;
        private Thread        _thread;
        private volatile bool _running;
        private readonly object _execLock = new object();
        private string _lastExecResponse = "";
        private long _lastExecEpochMs = 0;

        [NinjaScriptProperty]
        [Display(Name = "TCP Port", GroupName = "Python Server", Order = 1)]
        public int TcpPort { get; set; }

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name        = "PythonOrderServer";
                Description = "TCP order server — receives market orders from Python";
                TcpPort     = 5557;

                Calculate                    = Calculate.OnBarClose;
                IsExitOnSessionCloseStrategy = false;
                IsUnmanaged                  = true;
            }
            else if (State == State.DataLoaded)
            {
                StartServer();
            }
            else if (State == State.Terminated)
            {
                StopServer();
            }
        }

        protected override void OnBarUpdate() { }

        protected override void OnExecutionUpdate(
            Execution execution,
            string executionId,
            double price,
            int quantity,
            MarketPosition marketPosition,
            string orderId,
            DateTime time)
        {
            try
            {
                if (execution == null || execution.Order == null)
                    return;

                if (execution.Instrument == null || Instrument == null)
                    return;

                if (!string.Equals(execution.Instrument.FullName, Instrument.FullName, StringComparison.OrdinalIgnoreCase))
                    return;

                long epochMs = new DateTimeOffset(time.ToUniversalTime()).ToUnixTimeMilliseconds();
                string action = execution.Order.OrderAction.ToString().ToUpperInvariant();
                string localTime = time.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);

                string payload =
                    "LASTEXEC"
                    + "|epoch_ms=" + epochMs
                    + "|time_local=" + localTime
                    + "|price=" + price.ToString("0.########", CultureInfo.InvariantCulture)
                    + "|qty=" + quantity
                    + "|action=" + action
                    + "|market_position=" + marketPosition
                    + "|order_id=" + orderId
                    + "|execution_id=" + executionId
                    + "|instrument=" + execution.Instrument.FullName;

                lock (_execLock)
                {
                    _lastExecEpochMs = epochMs;
                    _lastExecResponse = payload;
                }

                Print("[PythonOrderServer] EXEC: " + payload);
            }
            catch (Exception ex)
            {
                Print("[PythonOrderServer] Execution capture error: " + ex.Message);
            }
        }

        // ── Server lifecycle ────────────────────────────────────────────────
        private void StartServer()
        {
            _running  = true;
            _listener = new TcpListener(IPAddress.Loopback, TcpPort);
            _listener.Start();
            _thread   = new Thread(AcceptLoop) { IsBackground = true, Name = "PythonOrderServer" };
            _thread.Start();
            Print("[PythonOrderServer] Listening on 127.0.0.1:" + TcpPort
                  + "  instrument=" + Instrument.FullName
                  + "  account=" + Account.Name);
        }

        private void StopServer()
        {
            _running = false;
            try { _listener?.Stop(); } catch { }
        }

        // ── Accept loop ─────────────────────────────────────────────────────
        private void AcceptLoop()
        {
            while (_running)
            {
                try
                {
                    TcpClient tcp = _listener.AcceptTcpClient();
                    new Thread(() => HandleClient(tcp)) { IsBackground = true }.Start();
                }
                catch (Exception ex)
                {
                    if (_running)
                        Print("[PythonOrderServer] Accept error: " + ex.Message);
                }
            }
        }

        // ── Per-connection handler ──────────────────────────────────────────
        private void HandleClient(TcpClient tcp)
        {
            try
            {
                using (tcp)
                using (var stream = tcp.GetStream())
                {
                    var buf = new byte[256];
                    int n   = stream.Read(buf, 0, buf.Length);
                    var cmd = Encoding.UTF8.GetString(buf, 0, n).Trim();

                    Print("[PythonOrderServer] CMD: " + cmd);
                    string result = Execute(cmd);
                    Print("[PythonOrderServer] RSP: " + result);

                    var resp = Encoding.UTF8.GetBytes(result + "\n");
                    stream.Write(resp, 0, resp.Length);
                }
            }
            catch (Exception ex)
            {
                Print("[PythonOrderServer] Client error: " + ex.Message);
            }
        }

        // ── Command execution ───────────────────────────────────────────────
        private string Execute(string cmd)
        {
            try
            {
                string[] parts  = cmd.Split('|');
                string   action = parts[0].Trim().ToUpperInvariant();

                if (action == "PING")
                    return "OK: PONG " + Instrument.FullName;

                if (action == "PRICE")
                {
                    double bid = GetCurrentBid();
                    double ask = GetCurrentAsk();
                    double last = Close != null && Close.Count > 0 ? Close[0] : double.NaN;
                    long epochMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                    string payload =
                        "PRICE"
                        + "|epoch_ms=" + epochMs
                        + "|last=" + last.ToString("0.########", CultureInfo.InvariantCulture)
                        + "|bid=" + bid.ToString("0.########", CultureInfo.InvariantCulture)
                        + "|ask=" + ask.ToString("0.########", CultureInfo.InvariantCulture)
                        + "|instrument=" + Instrument.FullName;

                    return "OK: " + payload;
                }

                if (action == "LASTEXEC")
                {
                    lock (_execLock)
                    {
                        if (_lastExecEpochMs <= 0 || string.IsNullOrEmpty(_lastExecResponse))
                            return "ERROR: no execution captured yet";
                        return "OK: " + _lastExecResponse;
                    }
                }

                if (action == "FLATTEN")
                {
                    Account.Flatten(new List<Instrument> { Instrument });
                    return "OK: FLATTEN " + Instrument.FullName;
                }

                if (parts.Length < 3)
                    return "ERROR: expected ACTION|DIRECTION|QTY";

                string direction = parts[1].ToUpper();
                if (!int.TryParse(parts[2], out int qty) || qty <= 0)
                    return "ERROR: qty must be a positive integer";

                OrderAction oa;
                if (action == "ENTRY")
                    oa = direction == "LONG" ? OrderAction.Buy : OrderAction.SellShort;
                else if (action == "EXIT")
                    oa = direction == "LONG" ? OrderAction.Sell : OrderAction.BuyToCover;
                else
                    return "ERROR: unknown action '" + action + "'";

                Order order = Account.CreateOrder(
                    Instrument,
                    oa,
                    OrderType.Market,
                    TimeInForce.Day,
                    qty,
                    0.0,
                    0.0,
                    "",
                    "PythonBot",
                    (CustomOrder)null
                );
                Account.Submit(new[] { order });
                return "OK: " + action + " " + direction + " " + qty + " " + Instrument.FullName;
            }
            catch (Exception ex)
            {
                return "ERROR: " + ex.Message;
            }
        }
    }
}
