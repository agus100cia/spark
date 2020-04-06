import argparse

##Captura de parametros
parser = argparse.ArgumentParser()
parser.add_argument('--anio', required=True, type=int)
parser.add_argument('--mes', required=True, type=int)
parametros = parser.parse_args()


print(parametros.anio)
print(parametros.mes)
