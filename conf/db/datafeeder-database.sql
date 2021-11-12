

CREATE SEQUENCE report_id_seq
    START WITH {{YEAR}}000000
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

CREATE TABLE report (
    id bigint DEFAULT nextval('report_id_seq'::regclass) NOT NULL,
    version smallint,
    report character varying(14) NOT NULL,
    clients smallint,
    pilots smallint,
    has_logs boolean,
    parsed boolean
);




CREATE SEQUENCE report_pilot_fp_remarks_id_seq
    START WITH {{YEAR}}00000000
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

CREATE TABLE report_pilot_fp_remarks (
    id bigint DEFAULT nextval('report_pilot_fp_remarks_id_seq'::regclass) NOT NULL,
    version smallint,
    remarks character varying(300)
);




CREATE SEQUENCE report_log_id_seq
    START WITH {{YEAR}}000000000
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

CREATE TABLE report_log (
    id bigint DEFAULT nextval('report_log_id_seq'::regclass) NOT NULL,
    version smallint,
    report_id bigint NOT NULL,
    section character varying(50),
    object character varying(50),
    message character varying(200),
    value character varying(1000)
);




CREATE SEQUENCE report_pilot_position_id_seq
    START WITH {{YEAR}}000000000
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

CREATE TABLE report_pilot_position (
    id bigint DEFAULT nextval('report_pilot_position_id_seq'::regclass) NOT NULL,
    version smallint,
    report_id bigint NOT NULL,
    pilot_number integer NOT NULL,
    callsign character varying(10),
    latitude real NOT NULL,
    longitude real NOT NULL,
    altitude integer NOT NULL,
    groundspeed smallint NOT NULL,
    heading smallint NOT NULL,
    fp_aircraft character varying(40),
    fp_origin character varying(4),
    fp_destination character varying(4),
    fp_remarks_id bigint,
    parsed_reg_no character varying(10),
    qnh_mb smallint,
    on_ground boolean
);


ALTER TABLE ONLY report
    ADD CONSTRAINT pk_report PRIMARY KEY (id);

ALTER TABLE ONLY report_pilot_fp_remarks
    ADD CONSTRAINT pk_report_pilot_fp_remarks PRIMARY KEY (id);

ALTER TABLE ONLY report_log
    ADD CONSTRAINT pk_report_log PRIMARY KEY (id);

ALTER TABLE ONLY report_pilot_position
    ADD CONSTRAINT pk_report_pilot_position PRIMARY KEY (id);

ALTER TABLE ONLY report
    ADD CONSTRAINT uk_report UNIQUE (report);

ALTER TABLE ONLY report_pilot_fp_remarks
    ADD CONSTRAINT uk_report_pilot_fp_remarks UNIQUE (remarks);

ALTER TABLE ONLY report_pilot_position
    ADD CONSTRAINT uk_report_pilot UNIQUE (report_id, pilot_number);

ALTER TABLE ONLY report_log
    ADD CONSTRAINT fk_report FOREIGN KEY (report_id) REFERENCES report(id);

ALTER TABLE ONLY report_pilot_position
    ADD CONSTRAINT fk_report FOREIGN KEY (report_id) REFERENCES report(id);

ALTER TABLE ONLY report_pilot_position
    ADD CONSTRAINT fk_report_pilot_position FOREIGN KEY (fp_remarks_id) REFERENCES report_pilot_fp_remarks(id);
