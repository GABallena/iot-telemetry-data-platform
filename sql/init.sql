
CREATE TABLE IF NOT EXISTS machines (
    machine_id   VARCHAR(20) PRIMARY KEY,
    factory_id   VARCHAR(20) NOT NULL,
    machine_type VARCHAR(50) NOT NULL,
    install_date DATE,
    status       VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS maintenance_logs (
    maintenance_id   VARCHAR(30) PRIMARY KEY,
    machine_id       VARCHAR(20) NOT NULL REFERENCES machines(machine_id),
    timestamp        TIMESTAMP NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    part_replaced    VARCHAR(100),
    downtime_minutes INTEGER DEFAULT 0,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS parts_replaced (
    id              SERIAL PRIMARY KEY,
    maintenance_id  VARCHAR(30) NOT NULL REFERENCES maintenance_logs(maintenance_id),
    part_name       VARCHAR(100) NOT NULL,
    part_number     VARCHAR(50),
    quantity        INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS planned_downtime (
    id          SERIAL PRIMARY KEY,
    machine_id  VARCHAR(20) NOT NULL REFERENCES machines(machine_id),
    start_time  TIMESTAMP NOT NULL,
    end_time    TIMESTAMP NOT NULL,
    reason      VARCHAR(200),
    approved_by VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_maint_machine ON maintenance_logs(machine_id);
CREATE INDEX IF NOT EXISTS idx_maint_ts ON maintenance_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_parts_maint ON parts_replaced(maintenance_id);
CREATE INDEX IF NOT EXISTS idx_planned_machine ON planned_downtime(machine_id);
