SELECT recipe_id,
       type
FROM octopus.recipe_tag
    CROSS APPLY STRING_SPLIT(type, ',')
    WHERE type <> ""
    ORDER BY 1
