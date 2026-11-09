-- One bounded counter per existing managed extension/operational instance,
-- not an action log. Preserve all existing journals, ciphertext and choices.
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

CREATE TRIGGER marketplace_choice_extension_created
AFTER INSERT ON marketplace_installation_metadata
BEGIN
    INSERT INTO marketplace_extension_choices(extension_id,revision) VALUES(NEW.extension_id,1)
    ON CONFLICT(extension_id) DO NOTHING;
END;
CREATE TRIGGER marketplace_choice_instance_created
AFTER INSERT ON extension_instances
WHEN EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=NEW.extension_id)
BEGIN
    INSERT INTO marketplace_instance_choices(instance_id,extension_id,revision)
        VALUES(NEW.instance_id,NEW.extension_id,1)
    ON CONFLICT(instance_id) DO NOTHING;
END;
-- Track even same-value writes: a later explicit disable supersedes an older
-- activation that observed the instance already disabled. Saturation/missing
-- counters fence NEW work permanently; they must never prevent turning OFF.
CREATE TRIGGER marketplace_choice_extension_changed
BEFORE UPDATE OF enabled ON extensions
WHEN EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=OLD.extension_id)
BEGIN
    UPDATE marketplace_extension_choices SET revision=CASE
        WHEN revision<9007199254740991 THEN revision+1 ELSE 9007199254740991 END
        WHERE extension_id=OLD.extension_id;
    INSERT INTO marketplace_extension_choices(extension_id,revision)
        SELECT OLD.extension_id,9007199254740991
        WHERE NOT EXISTS(SELECT 1 FROM marketplace_extension_choices WHERE extension_id=OLD.extension_id);
END;
CREATE TRIGGER marketplace_choice_instance_changed
BEFORE UPDATE OF enabled,instance_name,config_json ON extension_instances
WHEN EXISTS(SELECT 1 FROM marketplace_installation_metadata WHERE extension_id=OLD.extension_id)
BEGIN
    UPDATE marketplace_instance_choices SET revision=CASE
        WHEN revision<9007199254740991 THEN revision+1 ELSE 9007199254740991 END
        WHERE instance_id=OLD.instance_id;
    INSERT INTO marketplace_instance_choices(instance_id,extension_id,revision)
        SELECT OLD.instance_id,OLD.extension_id,9007199254740991
        WHERE NOT EXISTS(SELECT 1 FROM marketplace_instance_choices WHERE instance_id=OLD.instance_id);
END;
