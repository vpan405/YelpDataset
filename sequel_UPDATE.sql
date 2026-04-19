-- CALCULATING AND UPDATING BUSINESS TIP_COUNT
/* update business.tip_count
 - set tip_count for every business to the num of rows in Tip that references that respective business
 - businesses with no tips set to 0
 */
UPDATE Business
SET tip_count = (
    SELECT COUNT(*)
    FROM Tip
    WHERE Tip.b_id = Business.b_id
    );

/* update verification for business
 - 5 random businesses -> check if stored tip_count in business is same as actual count from Tip
 - returns b_id, name, stored tip_count, actual tip_count, and whether the values match
 */
SELECT b.b_id, b.name, b.tip_count as stored, COUNT(t.b_id) as actual, (b.tip_count = COUNT(t.b_id)) as match
FROM Business b
LEFT OUTER JOIN Tip t on b.b_id = t.b_id
WHERE b.b_id IN (
    SELECT b_id
    FROM business
    ORDER BY RANDOM() LIMIT 5
    )
GROUP BY b.b_id, b.name, b.tip_count;

-- CALCULATING AND UPDATING USERS TIPS
/* update users.tips
 - build temp table to store counts per user
 - update users from the temp table
 */
-- temp table
DROP TABLE temp_user_tips;

CREATE TABLE temp_user_tips AS
    SELECT user_id, COUNT(*) as tips_num
    FROM Tip
    GROUP BY user_id;

-- update from temp_user_tips
UPDATE Users u
    SET tips = ut.tips_num
    FROM temp_user_tips ut
    WHERE u.user_id = ut.user_id;

/* update verification for users
 - 5 random users -> check if stored tips in users is same as actual count from Tip
 - returns user_id, stored tips, actual tips, and whether the values match
 */
SELECT u.user_id, u.tips as stored, COUNT(t.user_id) as actual, (u.tips = COUNT(t.user_id)) as match
FROM Users u
LEFT OUTER JOIN Tip t on u.user_id = t.user_id
WHERE u.user_id IN (
    SELECT user_id
    FROM Users
    ORDER BY RANDOM() LIMIT 5
    )
GROUP BY u.user_id, u.tips;