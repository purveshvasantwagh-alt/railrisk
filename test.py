from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from railrisk.models.train import LiveTrainStatus
from railrisk.db.repository import save_snapshot, get_recent_snapshots

ist = ZoneInfo("Asia/Kolkata")
now = datetime.now(ist)

snapshot = LiveTrainStatus(
    train_number="12301",
    current_station="NDLS",
    target_station="CNB",
    scheduled_arrival=now + timedelta(hours=3),
    delay_minutes=15,
    halt_duration_mins=5,
    captured_at=now
)

# Test DB write and read
save_snapshot(snapshot)
recent = get_recent_snapshots("12301", limit=1)

print(f"DB Read Success: Retreived {len(recent)} snapshot(s)")
print(f"Station: {recent[0].current_station}, Delay: {recent[0].delay_minutes}m")