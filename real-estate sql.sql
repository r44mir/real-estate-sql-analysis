
-- Анализ рынка недвижимости.
-- Запрос очищает данные от выбросов, объединяет информацию о квартирах и объявлениях,
--  рассчитывает основные показатели по каждому городу:
-- количество объявлений, долю снятых с публикации, среднюю цену за квадратный метр,
-- среднюю площадь квартир и среднее время продажи.
-- Санкт-Петербург исключён из анализа, чтобы сравнить остальные города между собой.
-- В итоговой таблице выводятся города с достаточным количеством объявлений
-- (более 50), отсортированные по числу объявлени


-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND (
            (ceiling_height BETWEEN (SELECT ceiling_height_limit_l FROM limits) AND (SELECT ceiling_height_limit_h FROM limits))
            OR ceiling_height IS NULL
        )
),-- Выведем объявления без выбросов:
prepared_data AS (
    SELECT
        f.id,
        c.city AS city_name,
        CASE
            WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
            ELSE 'ЛенОбл'
        END AS region,
        CASE
            WHEN a.days_exposition BETWEEN 1 AND 30 THEN 'до месяца'
            WHEN a.days_exposition BETWEEN 31 AND 90 THEN 'до трёх месяцев'
            WHEN a.days_exposition BETWEEN 91 AND 180 THEN 'до полугода'
            WHEN a.days_exposition > 180 THEN 'более полугода'
        END AS activity_segment,
        f.total_area,
        f.rooms,
        f.balcony,
        f.floors_total,
        a.last_price,
        a.days_exposition,
        a.last_price / NULLIF(f.total_area, 0) AS price_per_sqm
    FROM real_estate.flats f
    JOIN real_estate.advertisement a ON f.id = a.id
    JOIN real_estate.city c ON f.city_id = c.city_id
    JOIN real_estate.type t ON f.type_id = t.type_id
    WHERE
        f.id IN (SELECT id FROM filtered_id)
        AND t.type = 'город'
        AND a.days_exposition IS NOT NULL
),
--Агрегация по региону и средние показатели
aggregated AS (
    SELECT
        region,
        activity_segment,
        COUNT(*) AS total_ads,
        ROUND(AVG(price_per_sqm)) AS avg_price_per_sqm,
        ROUND(AVG(total_area)) AS avg_total_area,
        ROUND(AVG(days_exposition)) AS avg_days_exposition
    FROM prepared_data
    GROUP BY region, activity_segment
)
SELECT
    region,
    activity_segment,
    total_ads AS "Количество объявлений",
    avg_price_per_sqm AS "Средняя стоимость кв. метра",
    avg_total_area AS "Средняя площадь",
    avg_days_exposition AS "Среднее количество дней продажи"
FROM aggregated
ORDER BY
    CASE region WHEN 'Санкт-Петербург' THEN 1 ELSE 2 END,
    CASE activity_segment
        WHEN 'до месяца' THEN 1
        WHEN 'до трёх месяцев' THEN 2
        WHEN 'до полугода' THEN 3
        WHEN 'более полугода' THEN 4
    END;

-- 2 Определяем границу выбросов
WITH limits AS (
    SELECT
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),-- Очистка от выбросов
filtered_flats AS (
    SELECT f.*, a.first_day_exposition, a.days_exposition, a.last_price, t.type, c.city
    FROM real_estate.flats f
    JOIN real_estate.advertisement a ON f.id = a.id
    JOIN real_estate.type t ON f.type_id = t.type_id
    JOIN real_estate.city c ON f.city_id = c.city_id
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND (
            (ceiling_height BETWEEN (SELECT ceiling_height_limit_l FROM limits) AND (SELECT ceiling_height_limit_h FROM limits))
            OR ceiling_height IS NULL
        )
        AND t.type = 'город'
), --Ежемесячно опубликовано
monthly_published AS (
    SELECT
        DATE_TRUNC('month', first_day_exposition) AS month,
        COUNT(*) AS total_ads,
        ROUND(AVG(last_price / NULLIF(total_area, 0))) AS avg_price_per_sqm,
        ROUND(AVG(total_area)) AS avg_area
    FROM filtered_flats
    GROUP BY month
), --Ежемесячно снято объявлений
monthly_removed AS (
    SELECT
        DATE_TRUNC('month', first_day_exposition + (days_exposition * INTERVAL '1 day')) AS month,
        COUNT(*) AS total_ads_removed,
        ROUND(AVG(last_price / NULLIF(total_area, 0))) AS avg_price_per_sqm_removed,
        ROUND(AVG(total_area)) AS avg_area_removed,
        ROUND(AVG(days_exposition)) AS avg_days_exposition
    FROM filtered_flats
    WHERE days_exposition IS NOT NULL
    GROUP BY month
)
SELECT
    p.month,
    p.total_ads,
    p.avg_price_per_sqm,
    p.avg_area,
    r.total_ads_removed,
    r.avg_price_per_sqm_removed,
    r.avg_area_removed,
    r.avg_days_exposition
FROM monthly_published p
LEFT JOIN monthly_removed r ON p.month = r.month
ORDER BY p.month;

-- 3
WITH limits AS (
    SELECT
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
--Фильтруем объявления
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND (
            (ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
             AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits))
            OR ceiling_height IS NULL
        )
),

-- Фильтруем квартиры
filtered_flats AS (
    SELECT
        f.*,
        a.last_price,
        a.days_exposition,
        c.city
    FROM real_estate.flats f
    JOIN real_estate.advertisement a ON f.id = a.id
    JOIN real_estate.city c ON f.city_id = c.city_id
    JOIN real_estate.type t ON f.type_id = t.type_id
    WHERE
        f.id IN (SELECT id FROM filtered_id)
        AND c.city != 'Санкт-Петербург'  -- 
),

--Основные метрики
city_stats AS (
    SELECT
        city AS "Город",
        COUNT(*) AS "Количество объявлений",
        SUM(CASE WHEN days_exposition IS NOT NULL THEN 1 ELSE 0 END) AS "Количество снятых с публикации",
        ROUND(100.0 * SUM(CASE WHEN days_exposition IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS "Доля снятых (%)",
        ROUND(AVG(NULLIF(last_price, 0) / NULLIF(total_area, 0))::numeric, 2) AS "Средняя цена за кв.м.",
        ROUND(AVG(total_area)::numeric, 2) AS "Средняя площадь",
        ROUND(AVG(days_exposition)::numeric, 2) AS "Среднее время продажи (дн.)"
    FROM filtered_flats
    GROUP BY city
    HAVING COUNT(*) > 50 -- только города с достаточным количеством объявлений
)

--Выводим топ-15 городов по количеству объявлений
SELECT *
FROM city_stats
ORDER BY "Количество объявлений" DESC
LIMIT 15;


