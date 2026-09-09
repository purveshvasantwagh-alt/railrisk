
# RailRisk 🚂

**RailRisk** is an engine and CLI utility designed for Indian Railways transit predictive risk analysis. It provides delay volatility calculations ($\Delta \text{delay} / \Delta t$), dynamic risk scoring via a secure AST-based sandbox, resilience through SQLite offline caching, and network retry handling.

---

## Quick Start & Setup

### Prerequisites

* **Python:** Version 3.10 or higher
* **Package Manager:** `pip`

### Step-by-Step Installation

1. **Clone the Repository:**
   ```powershell
   git clone [https://github.com/purveshvasantwagh-alt/railrisk.git](https://github.com/purveshvasantwagh-alt/railrisk.git)
   cd railrisk

```

2. **Set Up a Virtual Environment (Recommended):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

```


3. **Install Dependencies:**
```powershell
pip install requests tenacity pydantic pytest

```


4. **Run Initial Assessment:**
```powershell
$env:PYTHONPATH=".."
python main.py --train 12301 --target-station CNB --prep-time 45 --lead-time 15

```



---

## Key Features

* **IST Timezone Safety:** Strict `Asia/Kolkata` time normalization handling schedule calculations and midnight crossings.
* **Delay Volatility Engine:** Computes drift rates ($\Delta \text{delay} / \Delta t$) across historical running snapshots to detect delay acceleration.
* **AST Security Sandbox:** Dynamically evaluates user-defined mathematical scoring formulas without using unsafe `eval()` or `exec()` calls.
* **SQLite Offline Caching:** Persists snapshot payloads in an offline SQLite database with Write-Ahead Logging (WAL) for seamless fallback during network outages.
* **Network Retry Jitter:** Uses `tenacity` with exponential backoff and randomized jitter to handle rate limits and service downtime.
* **ASCII Dashboard Output:** Renders formatted terminal reports displaying train status, calculated margins, risk classifications (`LOW`, `MODERATE`, `CRITICAL`), and recommended vendor actions.

---

## Project Structure

```text
railrisk/
├── db/
│   ├── __init__.py
│   └── repository.py       # SQLite database persistence & WAL connection handling
├── engine/
│   ├── __init__.py
│   ├── dynamic_eval.py     # AST-based dynamic formula evaluation sandbox
│   ├── risk_calculator.py  # Margin calculation & risk level matrix
│   └── volatility.py       # Delay drift rate computation engine
├── models/
│   ├── __init__.py
│   ├── risk.py             # Risk assessment output models
│   └── train.py            # LiveTrainStatus Pydantic models
├── providers/
│   ├── __init__.py
│   └── ntes_provider.py    # NTES client with retry jitter & SQLite fallback
├── tests/
│   ├── __init__.py
│   └── test_predictor.py   # Unit test suite with mock network isolation
├── cli.py                  # Argument parser & ASCII terminal dashboard renderer
├── main.py                 # CLI entry point
├── test.py                 # Direct integration test runner
├── railrisk.db             # Offline SQLite database (auto-generated)
└── .gitignore              # Version control ignore rules

```

---

## CLI Usage & Configuration

### Command-Line Arguments

| Flag | Type | Default | Description |
| --- | --- | --- | --- |
| `--train` | `string` | **Required** | 5-digit Indian Railways train number |
| `--target-station` | `string` | **Required** | Station code (e.g., `CNB`, `NDLS`, `PNBE`) |
| `--prep-time` | `int` | `45` | Vendor order preparation time in minutes |
| `--lead-time` | `int` | `15` | Delivery lead time margin in minutes |
| `--formula` | `string` | `None` | Optional dynamic risk math expression |

---

## Dynamic AST Formula Evaluation

Pass custom scoring formulas directly via the `--formula` flag. The sandbox evaluates mathematical logic securely against contextual variables (`effective_buffer_mins`, `drift_rate_mph`, `delay_minutes`):

```powershell
$env:PYTHONPATH=".."
python main.py --train 12301 --target-station CNB --formula "(100 - effective_buffer_mins) + (drift_rate_mph * 0.5)"

```

---

## Testing & Quality Assurance

Run the unit test suite offline using `pytest`:

```powershell
$env:PYTHONPATH="."
pytest tests/ -v

```

The test suite validates:

* Timezone handling across midnight boundaries.
* Volatility drift calculations for train acceleration and deceleration.
* Risk matrix classification thresholds.
* AST sandbox execution and security blocking of forbidden constructs (`ast.Call`, `ast.Import`).
* NTES Provider fallback to local SQLite snapshots during mocked network outages.

---

## License

This project is licensed under the [MIT License](https://www.google.com/search?q=LICENSE).

```

```
