# Share Package Quick Start

This folder contains the current live engine plus the files it needs to run.

## 1) Install Python

Use Python 3.11 or newer.

## 2) Create the virtual environment

In Windows PowerShell:

```powershell
cd "<this-folder>"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 3) Configure `.env`

This package already includes a starter `.env`.

Check and edit these values before running:

- `NT_PORT` if your NinjaTrader TCP strategy is not using `5557`
- `NT_INSTRUMENT` to the current MNQ contract
- `IBKR_PORT` (`7497` paper, `7496` live)
- `ENABLE_ORDERS` / `DRY_RUN`
- Telegram settings if you want alerts

Safe defaults in the packaged `.env`:

- `ENABLE_ORDERS=false`
- `DRY_RUN=true`
- `STARTUP_FLATTEN=false`
- `IBKR_CLIENT_ID=99`

## 4) Validate connections

```powershell
.\.venv\Scripts\python.exe .\test_connections.py
```

## 5) Run the algo

```powershell
$env:PYTHONIOENCODING="utf-8"
.\.venv\Scripts\python.exe .\live_trader_v2.py
```

## Notes

- `PythonOrderServer.cs` must be installed and running inside NinjaTrader if using `BROKER=ninjatrader`.
- TWS / IB Gateway must have API socket access enabled.
- If the bot is started after `3:30 PM` New York time, it will not immediately flatten on startup.
- If the bot is already running before `3:30 PM` New York time, it will still perform the normal end-of-day flatten at `3:30 PM`.
