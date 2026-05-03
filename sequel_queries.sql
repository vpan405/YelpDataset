--Team Name: Sequel
--Members: Vanya Malik, Ritvik Garg, Victoria Pan

--Q1
--1a: List of unique categories of business in a particular city and state
SELECT DISTINCT(cat_name)
FROM BelongsTo
JOIN Business ON BelongsTo.b_id = Business.b_id
WHERE state = 'AZ' AND city = 'Scottsdale';

--2a: List of unique attributes of business in a particular city and state
SELECT DISTINCT(att_name)
FROM BusinessAttribute
JOIN Business ON BusinessAttribute.b_id = Business.b_id
WHERE state = 'AZ' AND city = 'Scottsdale';

--Q2 Businesses having a given set of categories

SELECT B.b_id, B.name, B.address, B.tip_count
FROM Business B
JOIN BelongsTo C ON C.b_id = B.b_id
JOIN BusinessAttribute A ON A.b_id = B.b_id
WHERE B.city = 'Scottsdale'
  AND B.state = 'AZ'
  AND C.cat_name IN ('Restaurants', 'Breakfast & Brunch', 'Bakeries')
GROUP BY B.b_id, B.name, B.address, B.tip_count
HAVING COUNT(DISTINCT C.cat_name) = 3
ORDER BY B.name;

--Q3 Businesses having a given set of attributes and values
SELECT B.b_id, B.name, B.address, B.tip_count
FROM Business B
JOIN BelongsTo C ON C.b_id = B.b_id
JOIN BusinessAttribute A ON A.b_id = B.b_id
WHERE B.city = 'Scottsdale'
  AND B.state = 'AZ'
  AND A.att_name IN ('BusinessAcceptsCreditCards', 'ByAppointmentOnly', 'WiFi')
  AND A.att_value IN ('True', 'free')
GROUP BY B.b_id, B.name, B.address, B.tip_count
HAVING COUNT(DISTINCT A.att_name) = 3
ORDER BY B.name;

--Q4 Business having a given set of attribute, values, and categories open at a certain time and day

(SELECT B.b_id, B.name, B.address, B.tip_count
FROM Business B
JOIN Hours H on B.b_id = H.b_id
WHERE H.open_time <= '10:30:00' AND H.close_time >= '13:30:00' AND H.weekday = 'Monday')
INTERSECT
(SELECT B.b_id, B.name, B.address, B.tip_count
FROM Business B
JOIN BelongsTo C ON C.b_id = B.b_id
JOIN BusinessAttribute A ON A.b_id = B.b_id
WHERE B.city = 'Scottsdale'
  AND B.state = 'AZ'
  AND C.cat_name IN ('Restaurants', 'Breakfast & Brunch', 'Bakeries')
GROUP BY B.b_id, B.name, B.address, B.tip_count
HAVING COUNT(DISTINCT C.cat_name) = 3)
INTERSECT
(SELECT B.b_id, B.name, B.address, B.tip_count
FROM Business B
JOIN BelongsTo C ON C.b_id = B.b_id
JOIN BusinessAttribute A ON A.b_id = B.b_id
WHERE B.city = 'Scottsdale'
  AND B.state = 'AZ'
  AND A.att_name IN ('BusinessAcceptsCreditCards', 'RestaurantsPriceRange2', 'WiFi')
  AND A.att_value IN ('True', 'free', '2')
GROUP BY B.b_id, B.name, B.address, B.tip_count
HAVING COUNT(DISTINCT A.att_name) = 3
)
ORDER BY name;

--Q5 count_categories: counts num of common categories for 2 businesses
CREATE OR REPLACE FUNCTION count_categories(id1 CHAR(22), id2 CHAR(22))
RETURNS INTEGER AS '
DECLARE
    count INTEGER;
BEGIN
    SELECT COUNT(*) INTO count
    FROM BelongsTo b1
    INNER JOIN BelongsTo b2 on b1.cat_name=b2.cat_name
    WHERE b1.b_id = id1 AND b2.b_id = id2;
    RETURN count;
END;
' LANGUAGE plpgsql;

SELECT count_categories('iPPzDL_oY8SJCjmycuXcVg', 'ncXQtqJT5Gk1QztwTrBrgw');

--Q6 geodistance: calculates distance btwn 2 coordinates using haversine formula
CREATE OR REPLACE FUNCTION geodistance(lat1 DOUBLE PRECISION, long1 DOUBLE PRECISION,
                                       lat2 DOUBLE PRECISION, long2 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS '
DECLARE
    radius DOUBLE PRECISION := 3958.8;
    dlat DOUBLE PRECISION;
    dlong DOUBLE PRECISION;
    a DOUBLE PRECISION;
    c DOUBLE PRECISION;
    dist DOUBLE PRECISION;
BEGIN
    -- convert degree deltas to radians
    dlat := RADIANS(lat2-lat1);
    dlong := RADIANS(long2-long1);

    -- haversine
    a := SIN(dlat / 2) ^ 2 + COS(RADIANS(lat1))*COS(RADIANS(lat2))*SIN(dlong / 2) ^ 2;
    c := 2 * ATAN2(SQRT(a), SQRT(1-a));
    dist := radius * c;
    RETURN dist;
END;
' LANGUAGE plpgsql;

SELECT geodistance(33.6399735577, -112.1334044052, 33.5796797, -111.9275444);

--Q7
SELECT Business.b_id, name, zip, count_categories('iPPzDL_oY8SJCjmycuXcVg', Business.b_id) as num_categories
FROM Business
WHERE geodistance((SELECT latitude FROM Business WHERE b_id = 'iPPzDL_oY8SJCjmycuXcVg'),
                         (SELECT longitude FROM Business WHERE b_id = 'iPPzDL_oY8SJCjmycuXcVg'),
                  latitude, longitude) <= 20
AND zip = (SELECT zip FROM Business WHERE b_id = 'iPPzDL_oY8SJCjmycuXcVg') AND b_id <> 'iPPzDL_oY8SJCjmycuXcVg'
ORDER BY num_categories DESC
LIMIT 15;

--Q8
SELECT Business.b_id, name, address, tip_count
FROM Business
JOIN BelongsTo ON Business.b_id = BelongsTo.b_id
WHERE cat_name = 'Restaurants' and zip = '85251' and
      tip_count = (SELECT max(tip_count) FROM Business
                                        JOIN BelongsTo on Business.b_id = BelongsTo.b_id
                                        WHERE (cat_name = 'Restaurants' and zip = '85251' ));


--Q9
SELECT t.user_id AS friend_id, u.name, t.time_stamp, t.tip_text FROM FriendOf f
JOIN Tip t ON f.user_followed = t.user_id
JOIN Users u ON t.user_id = u.user_id
WHERE F.user_following = 'TiWF94rl8Q6jqQf2YZSFPA'
ORDER BY T.time_stamp DESC
LIMIT 1;

--Q10
SELECT u.user_id, u.name, t1.time_stamp, t1.tip_text FROM FriendOf f
JOIN Users u ON f.user_followed = u.user_id
JOIN Tip t1 ON u.user_id = t1.user_id
WHERE f.user_following = 'TiWF94rl8Q6jqQf2YZSFPA'
AND t1.time_stamp = (
    SELECT MAX(t2.time_stamp) FROM Tip t2
    WHERE t2.user_id = U.user_id
)
ORDER BY t1.time_stamp DESC;

--Q11
CREATE OR REPLACE FUNCTION update_tip_counts()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE Business
    SET tip_count = tip_count + 1
    WHERE b_id = NEW.b_id;

    UPDATE Users
    SET tips = tips + 1
    WHERE user_id = NEW.user_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_tip
AFTER INSERT ON Tip
FOR EACH ROW
EXECUTE FUNCTION update_tip_counts();

-- tests
SELECT b_id, tip_count FROM Business WHERE b_id = 'uE6hI5_i4QVq12xU99xtqA';
SELECT user_id, tips FROM Users WHERE user_id = 'TiWF94rl8Q6jqQf2YZSFPA';

INSERT INTO Tip (time_stamp, num_likes, tip_text, b_id, user_id)
VALUES (CURRENT_TIMESTAMP, 0, 'Great coffee!', 'uE6hI5_i4QVq12xU99xtqA', 'TiWF94rl8Q6jqQf2YZSFPA');

SELECT b_id, tip_count FROM Business WHERE b_id = 'uE6hI5_i4QVq12xU99xtqA';
SELECT user_id, tips FROM Users WHERE user_id = 'TiWF94rl8Q6jqQf2YZSFPA';

--Q12
CREATE OR REPLACE FUNCTION validate_checkin_time()
RETURNS TRIGGER AS $$
DECLARE
    checkin_day VARCHAR;
    checkin_time TIME;
    is_open_now BOOLEAN;
BEGIN
    -- Extract day name (e.g., 'Monday') and time from timestamp
    checkin_day := TRIM(TO_CHAR(NEW.time_stamp, 'Day'));
    checkin_time := NEW.time_stamp::TIME;

    -- Check if the check-in falls within business hours
    SELECT EXISTS (
        SELECT 1 FROM Hours
        WHERE b_id = NEW.b_id
          AND weekday = checkin_day
          AND checkin_time >= open_time
          AND checkin_time <= close_time
    ) INTO is_open_now;

    IF NOT is_open_now THEN
        RAISE EXCEPTION 'Check-in failed: Business is currently closed on % at %', checkin_day, checkin_time;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER checkin_hours
BEFORE INSERT ON CheckIn
FOR EACH ROW
EXECUTE FUNCTION validate_checkin_time();

-- tests
INSERT INTO CheckIn (time_stamp, b_id)
VALUES ('2026-04-19 14:00:00', 'uE6hI5_i4QVq12xU99xtqA');

INSERT INTO CheckIn (time_stamp, b_id)
VALUES ('2026-04-20 23:00:00', 'uE6hI5_i4QVq12xU99xtqA');
