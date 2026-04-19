--Team Name: Sequel
--Members: Vanya Malik, Ritvik Garg, Victoria Pan
--Assumptions: BusinessAttributes is a weak entity and only has one value


DROP TABLE BelongsTo;
DROP TABLE Tip;
DROP TABLE FriendOf;
DROP TABLE Users;
DROP TABLE Hours;
DROP TABLE CheckIn;
DROP TABLE BusinessAttribute;
DROP TABLE BusinessCategory;
DROP TABLE Business CASCADE;


CREATE TABLE Business (
    b_id CHAR (22),
    rating REAL,
    name VARCHAR,
    city NAME,
    address VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    state VARCHAR,
    is_open BOOLEAN,
    zip VARCHAR,
    tip_count INTEGER,
    PRIMARY KEY (b_id)
);


CREATE TABLE Hours (
    weekday VARCHAR,
    b_id CHAR(22),
    open_time TIME,
    close_time TIME,
    PRIMARY KEY (weekday, b_id),
    FOREIGN KEY (b_id) REFERENCES Business(b_id)
);


CREATE TABLE BusinessCategory (
    cat_name VARCHAR,
    PRIMARY KEY (cat_name)
);


CREATE TABLE BusinessAttribute (
    att_name VARCHAR,
    att_value VARCHAR,
    b_id CHAR(22),
    PRIMARY KEY (att_name, b_id),
    FOREIGN KEY (b_id) REFERENCES Business(b_id)
);


CREATE TABLE CheckIn (
    time_stamp TIMESTAMP,
    b_id CHAR(22),
    PRIMARY KEY (time_stamp, b_id),
    FOREIGN KEY (b_id) REFERENCES Business(b_id)
);


CREATE TABLE Users (
    user_id CHAR (22),
    avg_stars FLOAT,
    create_date DATE,
    funny_score REAL,
    useful_score REAL,
    cool_score REAL,
    num_fans INTEGER,
    tips INTEGER,
    PRIMARY KEY (user_id)
);


CREATE TABLE FriendOf (
    user_following CHAR(22),
    user_followed CHAR(22),
    PRIMARY KEY (user_following, user_followed),
    FOREIGN KEY (user_followed) REFERENCES Users(user_id),
    FOREIGN KEY (user_following) REFERENCES Users(user_id)
);


CREATE TABLE Tip (
    time_stamp TIMESTAMP,
    num_likes INTEGER,
    tip_text VARCHAR,
    b_id CHAR(22),
    user_ID CHAR(22),
    PRIMARY KEY (time_stamp, b_id, user_id),
    FOREIGN KEY (b_id) REFERENCES Business(b_id),
    FOREIGN KEY (user_id) REFERENCES Users (user_id)
);


CREATE TABLE BelongsTo (
    cat_name VARCHAR,
    b_id CHAR(22),
    PRIMARY KEY (cat_name, b_id),
    FOREIGN KEY (cat_name) REFERENCES BusinessCategory(cat_name),
    FOREIGN KEY (b_id) REFERENCES Business(b_id)
);
