// 10 PANDEMIAS MAIS LETAIS;

SELECT 
    pw."Nome_Evento",
    pw."Ano_Inicio",
    pw."Regiao_Origem",
    pw."Mortes_Estimadas"
FROM pandemic_world pw 
ORDER BY "Mortes_Estimadas" DESC
LIMIT 10;

// 10 PANDEMIAS QUE MAIS SE ESPALHARAM

SELECT 
    "Nome_Evento",
    "Tipo_Patogeno",
    "Porcentagem_Disseminacao"
FROM pandemic_world pw 
ORDER BY "Porcentagem_Disseminacao" DESC
LIMIT 10;


// QUE MAIS TEVE IMPACTO ECONOMICO

SELECT 
    "Nome_Evento",
    "Ano_Inicio",
    "Ano_Fim",
    "Impacto_Economico_Bilhao_USD"
FROM pandemic_world pw 
WHERE "Impacto_Economico_Bilhao_USD" = (
    SELECT MAX("Impacto_Economico_Bilhao_USD")
    FROM pandemic_world
);

// EVENTOS ACIMA DA MEDIA EM TAXA DE FATALIDADE

SELECT 
    "Nome_Evento",
    "Mortes_Estimadas",
    "Taxa_Fatalidade"
FROM pandemic_world
WHERE "Taxa_Fatalidade" > (
    SELECT AVG("Taxa_Fatalidade")
    FROM pandemic_world
);

// MÉDIA DE DISSEMINAÇÃO E TOTAL DE MORTES POR TIPO DE PATÓGENO

SELECT 
    "Tipo_Patogeno",
    AVG("Porcentagem_Disseminacao") AS Media_Disseminacao,
    SUM("Mortes_Estimadas") AS Total_Mortes
FROM pandemic_world
GROUP BY "Tipo_Patogeno"
ORDER BY Total_Mortes DESC;

// IMPACTO POR CONTINENTE

SELECT 
    "Continentes_Afetados",
    "Nome_Evento",
    SUM("Mortes_Estimadas") AS Total_Mortes
FROM pandemic_world
GROUP BY "Continentes_Afetados", "Nome_Evento"
ORDER BY Total_Mortes DESC;

