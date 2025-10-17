Instalación y ejecución rápida
1. Requisitos
-Python 3.10+
-Cuenta en etherscan.io con API Key

2. Librerías: 
-requests, 
-pandas

3. Instalar dependencias
pip install requests pandas

4. Configurar la API Key
Para poder exportar datos de ethereum es necesario crearse una cuenta en etherscan, una vez creada se debe de generar una API KEY luego de esto podremos ponerla como variable de entorno
Windows (PowerShell):
setx ETHERSCAN_API_KEY "API_KEY_AQUI"

5. Ejecutar el script

En la carpeta del archivo etherscanV2.py, se tiene que ejecutar el siguiente comando:
python etherscanV2.py --chainid 1 --addresses "0x742d35Cc6634C0532925a3b844Bc454e4438f44e,0x28C6c06298d514Db089934071355E5743bf21d60,0x564286362092D8e7936f0549571a803B203aAceD" --min_rows 100000 --out eth_100k.csv

6. Resultado
Se genera el archivo eth_100k.csv con ~100k transacciones reales del blockchain Ethereum.
Luego puede cargarse a Polymer para análisis con IA.
