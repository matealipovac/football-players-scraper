--Add AgeCategory
ALTER TABLE football_players
ADD COLUMN AgeCategory VARCHAR;

UPDATE football_players
SET AgeCategory = CASE
    WHEN age <= 23 THEN 'Young'
    WHEN age BETWEEN 24 AND 32 THEN 'MidAge'
    WHEN age >= 33 THEN 'Old'
    ELSE NULL 
END;

-- Add GoalsPerClubGame
ALTER TABLE football_players
ADD COLUMN GoalsPerClubGame NUMERIC;

UPDATE football_players
SET GoalsPerClubGame = CASE
    WHEN number_of_appearances_in_current_club IS NOT NULL AND number_of_appearances_in_current_club != 0 THEN
        ROUND(CAST(goals_in_current_club AS NUMERIC) / CAST(number_of_appearances_in_current_club AS NUMERIC), 2)
    ELSE NULL
END;

--Task 2

SELECT
    current_club,
    ROUND(AVG(age), 2) AS avg_age,
    ROUND(AVG(number_of_appearances_in_current_club), 2) AS avg_appearances,
    COUNT(*) AS total_players
FROM
    football_players
GROUP BY
    current_club;


--Task 3
SELECT
    p1.current_club AS club,
    p1.name AS player_name,
    COUNT(p2.url) AS count_younger_higher_appearances
FROM
    football_players p1
LEFT JOIN
    football_players p2
ON
    p1.current_club <> p2.current_club
    AND p1.age > p2.age
    AND p1.positions = p2.positions
    AND p1.number_of_appearances_in_current_club < p2.number_of_appearances_in_current_club
WHERE
    p1.current_club = 'Real Madrid'  -- Specify the club
GROUP BY
    p1.current_club, p1.name;