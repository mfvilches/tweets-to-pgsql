# Descripción

Proyecto de Scrapping de Tweets continuo cada media hora con análisis de sentimientos VADAR con NTLK y guardado en una base de datos PostgreSQL. Este proyecto es parte de un proyecto mayor.
Por el momento, se puede rescatar la información de tweets de una ventana de dias antes de una fecha predeterminada de 6 ciudades de Chile (Santiago, Caldera, Punta Arenas, Concepción, La Serena y Con Con)

# Requisitos

Para realizar este proyecto es necesario contar con una Base de Datos PostgreSQL y una tabla con permisos de escritura.
La versión de Python debe ser al menos 3.9.12
Las librerías y sis versiones se encuentran en el archivo requirements.txt

# Archivo de configuración

Por razones de facilidad de configuración y seguridad, se cuenta con un archivo de configuración (config.py) donde se puede cambiar las fechas de búsqueda y el rango, como también agregar el string de conexión a la base de datos.

aaa
