# MLOps - Detección de Polvo Ambiental

## Introducción 
Mina Pirquitas SA es un proyecto minero ubicado en la Puna Jujeña. La mina produce un concentrado de plomo, plata y zinc para lo cual es necesario mover grandes volúmenes de roca (20Ktn/dia aproximadamente).

Mina Pirquitas SA posee una red de monitoreo ambiental la cual permite medir los impactos ambientales de la actividad. En el presente repositorio se trabajará solo con las mediciones de polvo ambiental (PM10 - material particulado menor a 10 micrones).

El proyecto minero cuenta con mediciones de PM10 desde el año 2008 hasta la actualidad. La frecuencia de monitoreo ha variado con el pasar de los años pero siempre al menos se han colecado 3 muestras por mes de cada sitio. A continuación se puede apreciar una figura con los sitios de monitoreo.

<img src='/img/2.png' alt = 'diagrama de trabajo' width= "800">

El objetivo de estas mediciones es conocer el impacto del proyecto minero en el ambiente en lo relacionado a la generación de emisiones de PM10. En el siguiente gráfico se puede apreciar las mediciones del sitio NP3 a lo largo del tiempo. Cabe destacar que el nivel regulatorio establece que por encima de los 150 ugr/m3 se considera un impacto que debe ser controlado. 

<img src='/img/3.png' alt = 'diagrama de trabajo' width= "800">

Las mediciones de PM10 están fuertemente relacionadas con las variables climaticas y con el impacto de la actividad minera. Es decir, los dias o las temporadas de mayor viento y sequedad es cuando mayores valores de PM10 se han registrado. 

Haciendo uso de herramientas de ML se intentará predecir si el valor de PM10 está por encima de 150 ug/m3 a partir de variables climaticas o meteorologias. Es necesario destacar que la empresa cuenta con una estación meteorológica con datos en tiempo real de las condiciones atmosféricas.

De esta manera y de manera conceptual el trabajo de MLOps busca entrenar un modelo a partir del registro historico de PM10 y las variables meteorológicas como lo muestra la siguiente figura.

<img src='/img/1.png' alt = 'diagrama de trabajo' width= "600">

## Objetivo

El principal objetivo es desarrollar un pipeline que ingeste los datos de PM10 y meteorología tanto en tiempo real como los historicos. Con esta información se entrenara un modelo de clasificación para predecir, a partir de los datos de la estación meteorológica, si el valor de PM10 estará por encima de los 150 ugr/m3.

Es necesario resaltar que de acuerdo con el método de muestro de PM10 es necesario contar con instrumentos especificos y con analisis de laborario lo cual demora en la obtención del resultado. Es decir, a la empresa de demanda al menos 20 dias entre que colectó una muestra de PM10 hasta que obtiene el resultado. Este tiempo no permite tomar decisiones a tiempo para controlar los impactos. 

De acuerdo a esto la principal importancia del modelo radica en estimar, con cierta insertidumbre conocida, a partir de las variables atmosféricas el comportamiento de la generación de polvo. Esto permitiría obtener información sincronizada con la estación meteorológica y no depender del resultado de laboratorio. 

## Dataset - Fuentes de información
Para desarrollar el trabajo se utilizaron dos fuentes de información primaria que posee la empresa:

    - Estación meteorologia online en Weatherlink Live. https://weatherlink.github.io/v2-api/api-reference
    - Tablero de Control Ambiental (TCA). Aplicativo web para almacenar los resultados de monitoreo. https://tca-ssrm.com/#/API

En ambos casos se accede desde una API. En el caso de Weatherlink Live fue necesario crear un algoritmo para la creación de la url. Una limitante de esta fuente de información es que solo permite obtener 24 horas de datos. Por este motivo se descargo de manera fisica un .txt y un .json con los datos historicos la meteorologia desde 2008 hasta el 13-11-2022 que es cuando se comenzó este trabajo. Luego se realizó una tarea especifica para actualizar los datos meteorologicos desde el 13-11-2022 en adelante. 

En el caso de la API del TCA solo es necesario consultar con un TOKEN de seguridad. 

## Proceso

Todo el proceso fue orquestado usando Airflow desde contenedores. Se trabajo primero de manera local y luego se realizo un deploy del trabajo en una instancia EC2 de AWS.

El modelo de ingesta de datos consiste en:

1. Consultar los datos a la API y persistirlos en un bucket de S2 denominado: ```rawdata```. 
2. Procesar y limpiar los datos persistidos en ```rawdata``` para luego almacenarlos en:
   1. Una tabla en una base de datos SQL.
   2. Carpeta del bucket denominada ```processed```. 

<img src='/img/4.png' alt = 'diagrama de trabajo' width= "600">
