import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
gc = gspread.service_account(filename='taller-tarea4-316219-4fa5a3aae996.json')
sh = gc.open_by_key('1b-Lu02PrnM1CNn5_OJYn8Jt1u39xy7Oxkpo7HB1mKKE')
worksheet = sh.get_worksheet(0)

paises = ['CHL','IRL', 'AUS', 'HRV', 'JPN', 'USA']
indicadores_pedidos = ['Number of deaths', 'Number of infant deaths','Number of under-five deaths',
'Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)', 
'Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)', 
'Estimates of number of homicides', 'Crude suicide rates (per 100 000 population)', 
'Mortality rate attributed to unintentional poisoning (per 100 000 population)', 
'Number of deaths attributed to non-communicable diseases, by type of disease and sex', 
'Estimated road traffic death rate (per 100 000 population)', 'Estimated number of road traffic deaths', 
'Mean BMI (kg/m&#xb2;) (crude estimate)', 'Mean BMI (kg/m&#xb2;) (age-standardized estimate)', 
'Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)', 
'Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)', 
'Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)', 
'Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)', 
'Prevalence of underweight among adults, BMI < 18.5 (crude estimate) (%)', 
'Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)', 
'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)', 'Estimate of daily cigarette smoking prevalence (%)', 
'Estimate of daily tobacco smoking prevalence (%)', 'Estimate of current tobacco smoking prevalence (%)', 
'Estimate of current cigarette smoking prevalence (%)', 'Mean systolic blood pressure (crude estimate)', 
'Mean fasting blood glucose (mmol/l) (crude estimate)', 'Mean Total Cholesterol (crude estimate)',
'Estimates of rates of homicides per 100 000 population']
datos_indicador = {'GHO':[], 'COUNTRY':[], 'YEAR':[], 'SEX':[], 'GHECAUSES':[], 'AGEGROUP':[], 'Display':[], 'Numeric':[], 'Low':[], 'High':[], 'Alcohol':[],'Tabaco':[], 'Cigarros':[], 'Presion': [], 'Glucosa': [], 'Colesterol': [], 'TMN': [], 'TMA': []}
datos_totales = []
for i in paises:
    response = requests.get(f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{i}.xml')
    tree = ET.fromstring(response.content)
    tree1 = ET.ElementTree(tree)
    root = tree1.getroot()
    for fact in root:
        if fact.find('GHO').text in indicadores_pedidos:
            datos = defaultdict(lambda: None)
            for i in fact:
                if i.tag in datos_indicador.keys():
                    if i.tag in ['Low', 'High', 'Numeric']:
                        datos[i.tag] = float(i.text)
                    else:
                        datos[i.tag] = i.text
            if datos['SEX'] not in ['Male', 'Female', 'Both sexes']:
                datos['SEX'] = 'Both sexes'

            if  'Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)' == datos['GHO']:
                datos['Alcohol'] = datos['Numeric']

            if 'Estimate of daily tobacco smoking prevalence (%)' == datos['GHO']:
                datos['Tabaco'] = datos['Numeric']

            if 'Estimate of daily cigarette smoking prevalence (%)' == datos['GHO']:
                datos['Cigarros'] = datos['Numeric']

            if 'Mean systolic blood pressure (crude estimate)' == datos['GHO']:
                datos['Presion'] = datos['Numeric']

            if 'Mean fasting blood glucose (mmol/l) (crude estimate)' == datos['GHO']:
                datos['Glucosa'] = datos['Numeric']

            if 'Mean Total Cholesterol (crude estimate)' == datos['GHO']:
                datos['Colesterol'] = datos['Numeric']

            datos_totales.append(datos)

for i in datos_totales:
    for j in datos_indicador.keys():
        datos_indicador[j].append(i[j])

tabla_final = pd.DataFrame(datos_indicador)
tabla_final['YEAR'] = pd.to_datetime(tabla_final['YEAR'], format='%Y')

set_with_dataframe(worksheet,tabla_final)
