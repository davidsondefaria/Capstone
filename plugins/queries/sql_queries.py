class SqlQueries:
    brazil_cities_stage_create = ("""
        DROP TABLE IF EXISTS brazil_cities_stage;
        CREATE TABLE IF NOT EXISTS brazil_cities_stage(
            city varchar(256),
            state varchar(20),
            capital boolean
            hdi_ranking int,
            hdi int,
            hdi_gni int,
            hdi_life int,
            hdi_education int,
            longitude numeric(18,0),
            latitude numeric(18,0),
            altitude numeric(18,0)
        );
    """)
    
    enem_stage_create = ("""
        DROP TABLE IF EXISTS enem_stage;
        CREATE TABLE IF NOT EXISTS enem_stage(
            registration numeric(18,0),
            city_residence_code int,
            city_residence varchar(256),
            state_residence_code int,
            state_residence varchar(2),
            age int,
            gender char,
            matiral_status varchar(16),
            color_race varchar(16),
            nationality varchar(16),
            high_school_status int,
            high_school_year_conclusion int,
            school_type varchar(16),
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
            def_other boolean,
            social_name boolean,
            city_test_code int,
            city_test varchar(256),
            state_test_code int,
            state_test varchar(2),
            presence_natural_science boolean,
            presence_human_science boolean,
            presence_languages ,boolean
            presence_math boolean,
            grade_natural_science numeric(4,1),
            grade_human_science numeric(4,1),
            grade_languages numeric(4,1),
            grade_math numeric(4,1),
            essay_status int,
            grade_essay numeric(4,1),
        );
    """)
    
    candidate_fact_create = ("""
        DROP TABLE IF EXISTS candidate_fact;
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
    """)
    candidate_dim_create = ("""
        DROP TABLE IF EXISTS candidate_dim;
        CREATE TABLE IF NOT EXISTS candidate_dim(
            id_test varchar(32) NOT NULL,
            presence_natural_science boolean,
            presence_human_science boolean,
            presence_languages boolean,
            presence_math boolean,
            city_test_code int,
            city_test varchar(256),
            state_test_code int,
            state_test varchar(2)
        );
    """)
    student_dim_create = ("""
        DROP TABLE IF EXISTS student_dim;
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
    """)
    special_dim_create = ("""
        DROP TABLE IF EXISTS special_dim;
        CREATE TABLE IF NOT EXISTS special_dim(
            id_special SERIAL NOT NULL,
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
    """)
    city_fact_create = ("""
        DROP TABLE IF EXISTS city_fact;
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
    """)
    city_dim_create = ("""
        DROP TABLE IF EXISTS city_dim;
        CREATE TABLE IF NOT EXISTS city_dim(
            id_city int NOT NULL,
            city varchar(256),
            state_code int,
            state varchar(2),
            capital boolean 
        );
    """)
    brazil_cities_stage_insert = ("""
        SELECT city,
            state,
            capital,
            hdi_ranking,
            hdi,
            hdi_gni,
            hdi_life,
            hdi_education,
            longitude,
            latitude,
            altitude
        FROM 
        WHERE 
    """)
    
    enem_stage_insert = ("""
        SELECT 
            registration,
            city_residence_code,
            city_residence,
            state_residence_code,
            state_residence,
            age,
            gender,
            matiral_status,
            color_race,
            nationality,
            high_school_status,
            high_school_year_conclusion,
            school_type,
            def_low_vision,
            def_blind,
            def_deaf,
            def_low_hearing,
            def_blind_deaf,
            def_physical,
            def_mental,
            def_attention,
            def_dyslexia,
            def_dyscalculia,
            def_autism,
            def_monocular_vision,
            def_other,
            social_name,
            city_test_code,
            city_test,
            state_test_code,
            state_test,
            presence_natural_science,
            presence_human_science,
            presence_languages,
            presence_math,
            grade_natural_science,
            grade_human_science,
            grade_languages,
            grade_math,
            essay_status,
            grade_essay
        FROM
        WHERE
    """)
    
    candidate_fact_insert = ("""
        SELECT 
            registration,
            id_city,
            id_city_test,
            id_test,
            high_school_year_conclusion,
            high_school_status,
            grade_natural_science,
            grade_human_science,
            grade_languages,
            grade_math,
            essay_status,
            grade_essay,
            CONSTRAINT id_candidate PRIMARY KEY (registration)
        FROM enem_stage
        WHERE
    """)

    candidate_dim_insert = ("""
        SELECT 
            md5(CONCAT(reg, city_test_code)) id_test,
            presence_natural_science,
            presence_human_science,
            presence_languages,
            presence_math,
            city_test_code,
            city_test,
            state_test_code,
            state_test
        FROM (SELECT TO_CHAR(registration) as reg, *
              FROM enem_stage
             ) as enem
    """)
    student_dim_insert = ("""
        SELECT
            md5(reg) registration,
            age,
            gender,
            collor,
            marital_status,
            nationality,
            school_type,
            social_name,
            city_residence_code,
            city_residence,
            state_residence_code,
            state_residence
        FROM (SELECT TO_CHAR(registration) as reg, *
              FROM enem_stage
             ) as enem
    """)
    special_dim_insert = ("""
        SELECT
            md5(reg) registration,
            age,
            gender,
            collor,
            marital_status,
            nationality,
            school_type,
            social_name,
            city_residence_code,
            city_residence,
            state_residence_code,
            state_residence
        FROM (SELECT TO_CHAR(registration) as reg, *
              FROM enem_stage
             ) as enem
    """)
    city_fact_insert = ("""
        SELECT
            id_special,
            def_low_vision,
            def_blind,
            def_deaf,
            def_low_hearing,
            def_blind_deaf,
            def_physical,
            def_mental,
            def_attention,
            def_dyslexia,
            def_dyscalculia,
            def_autism,
            def_monocular_vision,
            def_other
        FROM enem_stage
    """)
    city_dim_insert = ("""
        SELECT
            id_city
            city,
            state_code,
            state,
            capital
        FROM
        WHERE
    """)