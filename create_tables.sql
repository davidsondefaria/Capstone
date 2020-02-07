DROP TABLE IF EXISTS candidate_fact;
DROP TABLE IF EXISTS candidate_dim;
DROP TABLE IF EXISTS student_dim;
DROP TABLE IF EXISTS special_dim;
DROP TABLE IF EXISTS city_fact;
DROP TABLE IF EXISTS city_dim;

CREATE TABLE IF NOT EXISTS candidate_fact(
    registration numeric(18,0) NOT NULL,
    id_city int NOT NULL,
    id_city_test int NOT NULL,
    id_test int NOT NULL,
    high_school_year_conclusion int,
    high_school_status int,
    grade_natural_science numeric(4,1),
    grade_human_science numeric(4,1),
    grade_languages numeric(4,1),
    grade_math numeric(4,1),
    essay_status int,
    grade_essay numeric(4,1),
    CONSTRAINT id_candidate PRIMARY KEY (registration)
);

CREATE TABLE IF NOT EXISTS candidate_dim(
    id_test int NOT NULL,
    presence_natural_science boolean,
    presence_human_science boolean,
    presence_languages boolean,
    presence_math boolean,
    city_test_code int,
    city_test varchar(256),
    state_test_code int,
    state_test varchar(2)
);

CREATE TABLE IF NOT EXISTS student_dim(
    registration numeric(18,0) NOT NULL,
    age int,
    gender char,
    collor varchar(16),
    marital_status varchar(16),
    nationality varchar(16),
    school_type varchar(16),
    social_name boolean,
    city_residence_code int,
    city_residence varchar(256),
    state_residence_code int,
    state_residence varchar(2)
);

CREATE TABLE IF NOT EXISTS special_dim(
    id_special int NOT NULL,
    registration numeric(18,0) NOT NULL,
    def_low_vision boolean,
    def_blind boolean,
    def_deaf boolean,
    def_low_hearing boolean,
    def_blind_deaf boolean,
    def_physical boolean,
    def_mental boolean,
    def_attention boolean,
    def_dyslexia boolean,
    def_dyscalculia boolean,
    def_autism boolean,
    def_monocular_vision boolean,
    def_other boolean
);

CREATE TABLE IF NOT EXISTS city_fact(
    id_city_fact int NOT NULL,
    id_city int NOT NULL,
    hdi_ranking int,
    hdi int,
    hdi_gni int,
    hdi_life int,
    hdi_education int,
    longitude numeric(18,0),
    latitude numeric(18,0),
    altitude numeric(18,0),
);

CREATE TABLE IF NOT EXISTS city_dim(
    id_city int NOT NULL,
    city varchar(256),
    state_code int,
    state varchar(2),
    capital boolean 
);