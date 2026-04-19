"""
LMU Telemetry Recorder - POC
Records all available data from LMU shared memory to a DuckDB file.
Press Ctrl+C to stop recording.
"""

import sys
import time
import signal
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "lib")

import duckdb
from pyLMUSharedMemory import lmu_data
from pyLMUSharedMemory.lmu_mmap import MMapControl
from pyLMUSharedMemory.lmu_data import LMUConstants

# ── Config ────────────────────────────────────────────────────────────────────

POLL_RATE_HZ = 100                        # how often to read shared memory
BATCH_SIZE   = 100                        # samples to accumulate before writing
DB_PATH      = "lmu_telemetry.duckdb"    # output file

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lmu_recorder")

# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
-- Session metadata
CREATE TABLE IF NOT EXISTS sessions (
    session_id   INTEGER PRIMARY KEY,
    started_at   TIMESTAMP,
    track        TEXT,
    game_version INTEGER
);

-- One row per sample for the player vehicle (100Hz)
CREATE TABLE IF NOT EXISTS telemetry (
    session_id          INTEGER,
    sample_ts           DOUBLE,   -- elapsed time from game clock
    lap_number          INTEGER,
    lap_start_et        DOUBLE,
    time_into_lap       DOUBLE,
    lap_dist            DOUBLE,   -- from scoring (distance into lap)

    -- Position & motion
    pos_x               DOUBLE,
    pos_y               DOUBLE,
    pos_z               DOUBLE,
    vel_x               DOUBLE,
    vel_y               DOUBLE,
    vel_z               DOUBLE,
    accel_x             DOUBLE,
    accel_y             DOUBLE,
    accel_z             DOUBLE,

    -- Driver inputs (unfiltered = raw hardware, filtered = game processed)
    throttle            DOUBLE,
    brake               DOUBLE,
    steering            DOUBLE,
    clutch              DOUBLE,
    throttle_raw        DOUBLE,
    brake_raw           DOUBLE,
    steering_raw        DOUBLE,
    clutch_raw          DOUBLE,

    -- Powertrain
    gear                INTEGER,
    engine_rpm          DOUBLE,
    engine_max_rpm      DOUBLE,
    engine_torque       DOUBLE,
    engine_water_temp   DOUBLE,
    engine_oil_temp     DOUBLE,
    fuel                DOUBLE,
    fuel_capacity       DOUBLE,
    turbo_boost         DOUBLE,

    -- Aero
    drag                DOUBLE,
    front_downforce     DOUBLE,
    rear_downforce      DOUBLE,
    front_ride_height   DOUBLE,
    rear_ride_height    DOUBLE,

    -- Hybrid / ERS
    battery_charge      DOUBLE,
    state_of_charge     DOUBLE,
    virtual_energy      DOUBLE,
    regen               DOUBLE,
    motor_torque        DOUBLE,
    motor_rpm           DOUBLE,
    motor_temp          DOUBLE,
    motor_state         INTEGER,

    -- Driver aids state
    abs_active          BOOLEAN,
    tc_active           BOOLEAN,
    speed_limiter_active BOOLEAN,
    lap_invalidated     BOOLEAN,
    drs_state           BOOLEAN,
    abs_setting         INTEGER,
    tc_setting          INTEGER,
    motor_map           INTEGER,

    -- Gaps
    gap_car_ahead       DOUBLE,
    gap_car_behind      DOUBLE,

    -- Delta
    delta_best          DOUBLE,

    -- Tyre compounds
    front_compound      TEXT,
    rear_compound       TEXT,

    -- Wheel data (FL, FR, RL, RR)
    fl_brake_temp       DOUBLE,
    fr_brake_temp       DOUBLE,
    rl_brake_temp       DOUBLE,
    rr_brake_temp       DOUBLE,
    fl_tyre_wear        DOUBLE,
    fr_tyre_wear        DOUBLE,
    rl_tyre_wear        DOUBLE,
    rr_tyre_wear        DOUBLE,
    fl_tyre_pressure    DOUBLE,
    fr_tyre_pressure    DOUBLE,
    rl_tyre_pressure    DOUBLE,
    rr_tyre_pressure    DOUBLE,
    fl_susp_deflection  DOUBLE,
    fr_susp_deflection  DOUBLE,
    rl_susp_deflection  DOUBLE,
    rr_susp_deflection  DOUBLE,
    fl_ride_height      DOUBLE,
    fr_ride_height      DOUBLE,
    rl_ride_height      DOUBLE,
    rr_ride_height      DOUBLE,
    fl_rotation         DOUBLE,
    fr_rotation         DOUBLE,
    rl_rotation         DOUBLE,
    rr_rotation         DOUBLE,
    fl_lat_force        DOUBLE,
    fr_lat_force        DOUBLE,
    rl_lat_force        DOUBLE,
    rr_lat_force        DOUBLE,
    fl_lon_force        DOUBLE,
    fr_lon_force        DOUBLE,
    rl_lon_force        DOUBLE,
    rr_lon_force        DOUBLE,
    fl_grip_fract       DOUBLE,
    fr_grip_fract       DOUBLE,
    rl_grip_fract       DOUBLE,
    rr_grip_fract       DOUBLE,
    fl_surface_type     INTEGER,
    fr_surface_type     INTEGER,
    rl_surface_type     INTEGER,
    rr_surface_type     INTEGER
);

-- One row per sample per OTHER vehicle in session (from scoring, ~10Hz)
CREATE TABLE IF NOT EXISTS other_vehicles (
    session_id          INTEGER,
    sample_ts           DOUBLE,
    vehicle_id          INTEGER,
    driver_name         TEXT,
    vehicle_name        TEXT,
    vehicle_class       TEXT,
    place               INTEGER,
    lap_number          INTEGER,
    lap_dist            DOUBLE,
    time_into_lap       DOUBLE,
    pos_x               DOUBLE,
    pos_y               DOUBLE,
    pos_z               DOUBLE,
    vel_x               DOUBLE,
    vel_y               DOUBLE,
    vel_z               DOUBLE,
    best_lap_time       DOUBLE,
    last_lap_time       DOUBLE,
    best_sector1        DOUBLE,
    best_sector2        DOUBLE,
    last_sector1        DOUBLE,
    last_sector2        DOUBLE,
    in_pits             BOOLEAN,
    pit_state           INTEGER,
    flag                INTEGER,
    under_yellow        BOOLEAN,
    fuel_fraction       INTEGER,
    drs_state           BOOLEAN,
    attack_mode         INTEGER,
    steam_id            INTEGER
);

-- Session-level scoring snapshots
CREATE TABLE IF NOT EXISTS scoring_snapshots (
    session_id              INTEGER,
    sample_ts               DOUBLE,
    session_type            INTEGER,
    game_phase              INTEGER,
    yellow_flag_state       INTEGER,
    ambient_temp            DOUBLE,
    track_temp              DOUBLE,
    raining                 DOUBLE,
    dark_cloud              DOUBLE,
    avg_path_wetness        DOUBLE,
    min_path_wetness        DOUBLE,
    max_path_wetness        DOUBLE,
    session_time_remaining  DOUBLE,
    time_of_day             DOUBLE,
    track_grip_level        INTEGER,
    num_vehicles            INTEGER
);
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def decode(b) -> str:
    """Safely decode bytes to string"""
    try:
        return b.decode("utf-8", errors="replace").rstrip("\x00")
    except Exception:
        return ""


def extract_telemetry(data, session_id: int, scoring_ts: float, lap_dist: float, time_into_lap: float) -> dict:
    """Extract player telemetry row from shared memory snapshot"""
    t = data.telemetry
    if not t.playerHasVehicle:
        return None

    idx = t.playerVehicleIdx
    v   = t.telemInfo[idx]
    w   = v.mWheels  # FL=0, FR=1, RL=2, RR=3

    return {
        "session_id":           session_id,
        "sample_ts":            v.mElapsedTime,
        "lap_number":           v.mLapNumber,
        "lap_start_et":         v.mLapStartET,
        "time_into_lap":        time_into_lap,
        "lap_dist":             lap_dist,
        "pos_x":                v.mPos.x,
        "pos_y":                v.mPos.y,
        "pos_z":                v.mPos.z,
        "vel_x":                v.mLocalVel.x,
        "vel_y":                v.mLocalVel.y,
        "vel_z":                v.mLocalVel.z,
        "accel_x":              v.mLocalAccel.x,
        "accel_y":              v.mLocalAccel.y,
        "accel_z":              v.mLocalAccel.z,
        "throttle":             v.mFilteredThrottle,
        "brake":                v.mFilteredBrake,
        "steering":             v.mFilteredSteering,
        "clutch":               v.mFilteredClutch,
        "throttle_raw":         v.mUnfilteredThrottle,
        "brake_raw":            v.mUnfilteredBrake,
        "steering_raw":         v.mUnfilteredSteering,
        "clutch_raw":           v.mUnfilteredClutch,
        "gear":                 v.mGear,
        "engine_rpm":           v.mEngineRPM,
        "engine_max_rpm":       v.mEngineMaxRPM,
        "engine_torque":        v.mEngineTorque,
        "engine_water_temp":    v.mEngineWaterTemp,
        "engine_oil_temp":      v.mEngineOilTemp,
        "fuel":                 v.mFuel,
        "fuel_capacity":        v.mFuelCapacity,
        "turbo_boost":          v.mTurboBoostPressure,
        "drag":                 v.mDrag,
        "front_downforce":      v.mFrontDownforce,
        "rear_downforce":       v.mRearDownforce,
        "front_ride_height":    v.mFrontRideHeight,
        "rear_ride_height":     v.mRearRideHeight,
        "battery_charge":       v.mBatteryChargeFraction,
        "state_of_charge":      v.mStateOfCharge,
        "virtual_energy":       v.mVirtualEnergy,
        "regen":                v.mRegen,
        "motor_torque":         v.mElectricBoostMotorTorque,
        "motor_rpm":            v.mElectricBoostMotorRPM,
        "motor_temp":           v.mElectricBoostMotorTemperature,
        "motor_state":          v.mElectricBoostMotorState,
        "abs_active":           v.mABSActive,
        "tc_active":            v.mTCActive,
        "speed_limiter_active": v.mSpeedLimiterActive,
        "lap_invalidated":      v.mLapInvalidated,
        "drs_state":            v.mDeltaBest,  # placeholder until confirmed field
        "abs_setting":          v.mABS,
        "tc_setting":           v.mTC,
        "motor_map":            v.mMotorMap,
        "gap_car_ahead":        v.mTimeGapCarAhead,
        "gap_car_behind":       v.mTimeGapCarBehind,
        "delta_best":           v.mDeltaBest,
        "front_compound":       decode(v.mFrontTireCompoundName),
        "rear_compound":        decode(v.mRearTireCompoundName),
        "fl_brake_temp":        w[0].mBrakeTemp,
        "fr_brake_temp":        w[1].mBrakeTemp,
        "rl_brake_temp":        w[2].mBrakeTemp,
        "rr_brake_temp":        w[3].mBrakeTemp,
        "fl_tyre_wear":         w[0].mWear,
        "fr_tyre_wear":         w[1].mWear,
        "rl_tyre_wear":         w[2].mWear,
        "rr_tyre_wear":         w[3].mWear,
        "fl_tyre_pressure":     w[0].mPressure,
        "fr_tyre_pressure":     w[1].mPressure,
        "rl_tyre_pressure":     w[2].mPressure,
        "rr_tyre_pressure":     w[3].mPressure,
        "fl_susp_deflection":   w[0].mSuspensionDeflection,
        "fr_susp_deflection":   w[1].mSuspensionDeflection,
        "rl_susp_deflection":   w[2].mSuspensionDeflection,
        "rr_susp_deflection":   w[3].mSuspensionDeflection,
        "fl_ride_height":       w[0].mRideHeight,
        "fr_ride_height":       w[1].mRideHeight,
        "rl_ride_height":       w[2].mRideHeight,
        "rr_ride_height":       w[3].mRideHeight,
        "fl_rotation":          w[0].mRotation,
        "fr_rotation":          w[1].mRotation,
        "rl_rotation":          w[2].mRotation,
        "rr_rotation":          w[3].mRotation,
        "fl_lat_force":         w[0].mLateralForce,
        "fr_lat_force":         w[1].mLateralForce,
        "rl_lat_force":         w[2].mLateralForce,
        "rr_lat_force":         w[3].mLateralForce,
        "fl_lon_force":         w[0].mLongitudinalForce,
        "fr_lon_force":         w[1].mLongitudinalForce,
        "rl_lon_force":         w[2].mLongitudinalForce,
        "rr_lon_force":         w[3].mLongitudinalForce,
        "fl_grip_fract":        w[0].mGripFract,
        "fr_grip_fract":        w[1].mGripFract,
        "rl_grip_fract":        w[2].mGripFract,
        "rr_grip_fract":        w[3].mGripFract,
        "fl_surface_type":      w[0].mSurfaceType,
        "fr_surface_type":      w[1].mSurfaceType,
        "rl_surface_type":      w[2].mSurfaceType,
        "rr_surface_type":      w[3].mSurfaceType,
    }


def extract_other_vehicles(data, session_id: int, sample_ts: float, player_id: int) -> list[dict]:
    """Extract all non-player vehicle rows from scoring"""
    rows = []
    s = data.scoring
    num = s.scoringInfo.mNumVehicles

    for i in range(num):
        v = s.vehScoringInfo[i]
        if v.mID == player_id:
            continue
        rows.append({
            "session_id":     session_id,
            "sample_ts":      sample_ts,
            "vehicle_id":     v.mID,
            "driver_name":    decode(v.mDriverName),
            "vehicle_name":   decode(v.mVehicleName),
            "vehicle_class":  decode(v.mVehicleClass),
            "place":          v.mPlace,
            "lap_number":     v.mTotalLaps,
            "lap_dist":       v.mLapDist,
            "time_into_lap":  v.mTimeIntoLap,
            "pos_x":          v.mPos.x,
            "pos_y":          v.mPos.y,
            "pos_z":          v.mPos.z,
            "vel_x":          v.mLocalVel.x,
            "vel_y":          v.mLocalVel.y,
            "vel_z":          v.mLocalVel.z,
            "best_lap_time":  v.mBestLapTime,
            "last_lap_time":  v.mLastLapTime,
            "best_sector1":   v.mBestSector1,
            "best_sector2":   v.mBestSector2,
            "last_sector1":   v.mLastSector1,
            "last_sector2":   v.mLastSector2,
            "in_pits":        v.mInPits,
            "pit_state":      v.mPitState,
            "flag":           v.mFlag,
            "under_yellow":   v.mUnderYellow,
            "fuel_fraction":  v.mFuelFraction,
            "drs_state":      v.mDRSState,
            "attack_mode":    v.mAttackMode,
            "steam_id":       v.mSteamID,
        })
    return rows


def extract_scoring_snapshot(data, session_id: int, sample_ts: float) -> dict:
    """Extract session-level scoring snapshot"""
    si = data.scoring.scoringInfo
    return {
        "session_id":             session_id,
        "sample_ts":              sample_ts,
        "session_type":           si.mSession,
        "game_phase":             si.mGamePhase,
        "yellow_flag_state":      si.mYellowFlagState,
        "ambient_temp":           si.mAmbientTemp,
        "track_temp":             si.mTrackTemp,
        "raining":                si.mRaining,
        "dark_cloud":             si.mDarkCloud,
        "avg_path_wetness":       si.mAvgPathWetness,
        "min_path_wetness":       si.mMinPathWetness,
        "max_path_wetness":       si.mMaxPathWetness,
        "session_time_remaining": si.mSessionTimeRemaining,
        "time_of_day":            si.mTimeOfDay,
        "track_grip_level":       si.mTrackGripLevel,
        "num_vehicles":           si.mNumVehicles,
    }


# ── Batch writer ──────────────────────────────────────────────────────────────

class BatchWriter:
    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.telem_batch:   list[dict] = []
        self.other_batch:   list[dict] = []
        self.scoring_batch: list[dict] = []

    def add_telemetry(self, row: dict):
        self.telem_batch.append(row)
        if len(self.telem_batch) >= BATCH_SIZE:
            self.flush_telemetry()

    def add_other_vehicles(self, rows: list[dict]):
        self.other_batch.extend(rows)
        if len(self.other_batch) >= BATCH_SIZE:
            self.flush_other()

    def add_scoring(self, row: dict):
        self.scoring_batch.append(row)
        if len(self.scoring_batch) >= BATCH_SIZE:
            self.flush_scoring()

    def flush_telemetry(self):
        if self.telem_batch:
            self.con.executemany(
                f"INSERT INTO telemetry VALUES ({','.join(['?']*len(self.telem_batch[0]))})",
                [list(r.values()) for r in self.telem_batch]
            )
            self.telem_batch.clear()

    def flush_other(self):
        if self.other_batch:
            self.con.executemany(
                f"INSERT INTO other_vehicles VALUES ({','.join(['?']*len(self.other_batch[0]))})",
                [list(r.values()) for r in self.other_batch]
            )
            self.other_batch.clear()

    def flush_scoring(self):
        if self.scoring_batch:
            self.con.executemany(
                f"INSERT INTO scoring_snapshots VALUES ({','.join(['?']*len(self.scoring_batch[0]))})",
                [list(r.values()) for r in self.scoring_batch]
            )
            self.scoring_batch.clear()

    def flush_all(self):
        self.flush_telemetry()
        self.flush_other()
        self.flush_scoring()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Opening shared memory...")
    info = MMapControl(LMUConstants.LMU_SHARED_MEMORY_FILE, lmu_data.LMUObjectOut)

    try:
        info.create(0)  # copy access mode - safer for reading
    except Exception as e:
        log.error("Could not open shared memory: %s", e)
        log.error("Is LMU running? Is 'Enable Plugins' ON in LMU Settings -> Gameplay?")
        sys.exit(1)

    log.info("Connected to shared memory")

    # Wait for game to be active
    log.info("Waiting for active session...")
    while True:
        info.update()
        if info.data.generic.gameVersion:
            break
        time.sleep(0.5)

    # Read session info
    info.update()
    track        = decode(info.data.scoring.scoringInfo.mTrackName)
    game_version = info.data.generic.gameVersion
    log.info("Track: %s | Game version: %s", track, game_version)

    # Find player vehicle ID
    player_id = INVALID_INDEX = -1
    s = info.data.scoring
    for i in range(s.scoringInfo.mNumVehicles):
        if s.vehScoringInfo[i].mIsPlayer:
            player_id = s.vehScoringInfo[i].mID
            break

    # Setup DuckDB
    log.info("Opening DuckDB at %s", DB_PATH)
    con = duckdb.connect(DB_PATH)
    con.execute(SCHEMA)

    # Create session record
    con.execute(
        "INSERT INTO sessions VALUES (nextval('seq_session'), ?, ?, ?)"
        if False else  # DuckDB sequence syntax varies, use simpler approach
        "INSERT INTO sessions SELECT COALESCE(MAX(session_id),0)+1, ?, ?, ? FROM sessions",
        [datetime.now(), track, game_version]
    )
    session_id = con.execute("SELECT MAX(session_id) FROM sessions").fetchone()[0]
    log.info("Session ID: %s", session_id)

    writer = BatchWriter(con)

    # Tracking for scoring sample rate (we don't need scoring at 100Hz)
    last_scoring_ts   = 0.0
    SCORING_INTERVAL  = 0.1  # write scoring snapshot every 100ms
    last_other_ts     = 0.0
    OTHER_INTERVAL    = 0.1

    sample_count = 0
    interval     = 1.0 / POLL_RATE_HZ
    running      = True

    def handle_stop(sig, frame):
        nonlocal running
        log.info("Stopping...")
        running = False

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    log.info("Recording at %dHz — press Ctrl+C to stop", POLL_RATE_HZ)

    while running:
        loop_start = time.perf_counter()

        info.update()
        data = info.data

        if not data.generic.gameVersion:
            time.sleep(0.5)
            continue

        current_et = data.scoring.scoringInfo.mCurrentET

        # ── Player telemetry (every sample) ──
        player_scoring = None
        s = data.scoring
        for i in range(s.scoringInfo.mNumVehicles):
            if s.vehScoringInfo[i].mID == player_id or s.vehScoringInfo[i].mIsPlayer:
                player_scoring = s.vehScoringInfo[i]
                player_id      = s.vehScoringInfo[i].mID
                break

        lap_dist      = player_scoring.mLapDist     if player_scoring else 0.0
        time_into_lap = player_scoring.mTimeIntoLap if player_scoring else 0.0

        row = extract_telemetry(data, session_id, current_et, lap_dist, time_into_lap)
        if row:
            writer.add_telemetry(row)
            sample_count += 1

        # ── Scoring snapshot (throttled) ──
        if current_et - last_scoring_ts >= SCORING_INTERVAL:
            snap = extract_scoring_snapshot(data, session_id, current_et)
            writer.add_scoring(snap)
            last_scoring_ts = current_et

        # ── Other vehicles (throttled) ──
        if current_et - last_other_ts >= OTHER_INTERVAL:
            other_rows = extract_other_vehicles(data, session_id, current_et, player_id)
            if other_rows:
                writer.add_other_vehicles(other_rows)
            last_other_ts = current_et

        if sample_count % 500 == 0 and sample_count > 0:
            log.info("Samples recorded: %d | ET: %.1fs | Lap: %d",
                     sample_count,
                     current_et,
                     row.get("lap_number", 0) if row else 0)

        # ── Sleep remainder of interval ──
        elapsed = time.perf_counter() - loop_start
        sleep_t = interval - elapsed
        if sleep_t > 0:
            time.sleep(sleep_t)

    # ── Shutdown ──────────────────────────────────────────────────────────────
    log.info("Flushing remaining data...")
    writer.flush_all()
    info.close()
    con.close()
    log.info("Done. Recorded %d telemetry samples to %s", sample_count, DB_PATH)


if __name__ == "__main__":
    main()