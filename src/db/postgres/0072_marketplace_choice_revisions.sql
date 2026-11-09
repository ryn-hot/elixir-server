-- Additive only: old journals/ciphertext/operational choices are not rewritten.
CREATE TABLE marketplace_extension_choices (
    extension_id TEXT PRIMARY KEY REFERENCES marketplace_installation_metadata(extension_id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991)
);
CREATE TABLE marketplace_instance_choices (
    instance_id TEXT PRIMARY KEY REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL REFERENCES marketplace_installation_metadata(extension_id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991)
);
CREATE INDEX marketplace_instance_choices_extension ON marketplace_instance_choices(extension_id);
INSERT INTO marketplace_extension_choices(extension_id,revision)
    SELECT extension_id,1 FROM marketplace_installation_metadata;
INSERT INTO marketplace_instance_choices(instance_id,extension_id,revision)
    SELECT i.instance_id,i.extension_id,1 FROM extension_instances i
    JOIN marketplace_installation_metadata m ON m.extension_id=i.extension_id;

CREATE FUNCTION marketplace_choice_extension_created() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO marketplace_extension_choices(extension_id,revision) VALUES(NEW.extension_id,1)
    ON CONFLICT(extension_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER marketplace_choice_extension_created
AFTER INSERT ON marketplace_installation_metadata
FOR EACH ROW EXECUTE FUNCTION marketplace_choice_extension_created();

CREATE FUNCTION marketplace_choice_instance_created() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=NEW.extension_id) THEN
        INSERT INTO marketplace_instance_choices(instance_id,extension_id,revision)
            VALUES(NEW.instance_id,NEW.extension_id,1)
        ON CONFLICT(instance_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER marketplace_choice_instance_created
AFTER INSERT ON extension_instances
FOR EACH ROW EXECUTE FUNCTION marketplace_choice_instance_created();

-- Parent row is locked by the operational write BEFORE touching its counter.
-- Same-value writes count; saturation/missing counters fence NEW work, not OFF.
CREATE FUNCTION marketplace_choice_extension_changed() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=OLD.extension_id) THEN
        UPDATE marketplace_extension_choices SET revision=CASE
            WHEN revision<9007199254740991 THEN revision+1 ELSE 9007199254740991 END
            WHERE extension_id=OLD.extension_id;
        IF NOT FOUND THEN
            INSERT INTO marketplace_extension_choices(extension_id,revision)
                VALUES(OLD.extension_id,9007199254740991)
            ON CONFLICT(extension_id) DO UPDATE SET revision=9007199254740991;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER marketplace_choice_extension_changed
BEFORE UPDATE OF enabled ON extensions
FOR EACH ROW EXECUTE FUNCTION marketplace_choice_extension_changed();

CREATE FUNCTION marketplace_choice_instance_changed() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=OLD.extension_id) THEN
        UPDATE marketplace_instance_choices SET revision=CASE
            WHEN revision<9007199254740991 THEN revision+1 ELSE 9007199254740991 END
            WHERE instance_id=OLD.instance_id;
        IF NOT FOUND THEN
            INSERT INTO marketplace_instance_choices(instance_id,extension_id,revision)
                VALUES(OLD.instance_id,OLD.extension_id,9007199254740991)
            ON CONFLICT(instance_id) DO UPDATE SET revision=9007199254740991;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER marketplace_choice_instance_changed
BEFORE UPDATE OF enabled,instance_name,config_json ON extension_instances
FOR EACH ROW EXECUTE FUNCTION marketplace_choice_instance_changed();
