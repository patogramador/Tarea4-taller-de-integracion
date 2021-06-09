import requests
import xml.etree.ElementTree as et
import pandas as pd
from collections import defaultdict
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import date

paises = ["AUS", "USA", "CHL", "NLD", "IRL", "JPN"]
atributos = ["GHO", "YEAR", "Display", "COUNTRY", "SEX", "GHECAUSES", "AGEGROUP", "Numeric", "Low", "High",
             "Presion arterial", "Glucosa", "Colesterol", "Alcohol", "Cigarros", "Tabaco"]
indicadores = ["Number of deaths", "Number of infant deaths",
               "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
               "Number of under-five deaths",
               "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)", "Estimates of number of homicides",
               "Crude suicide rates (per 100 000 population)", "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
               "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
               "Estimated road traffic death rate (per 100 000 population)", "Estimated number of road traffic deaths",
               "Mean BMI (crude estimate)", "Mean BMI (age-standardized estimate)", "Prevalence of obesity among adults, BMI > 30 (age-standardized estimate) (%)",
               "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
               "Prevalence of overweight among adults, BMI > 25 (age-standardized estimate) (%)", "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
               "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)", "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
               "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)", "Estimate of daily cigarette smoking prevalence (%)",
               "Estimate of daily tobacco smoking prevalence (%)", "Estimate of current cigarette smoking prevalence (%)",
               "Estimate of current tobacco smoking prevalence (%)", "Mean systolic blood pressure (crude estimate)",
               "Mean fasting blood glucose (mmol/l) (crude estimate)", "Mean Total Cholesterol (crude estimate)", "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)",
               "Mean BMI (kg/m&#xb2;) (crude estimate)", "Mean BMI (kg/m&#xb2;) (age-standardized estimate)", "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
               "Estimates of rates of homicides per 100 000 population"]
dic = dict()
contador = 0
for pais in paises:
    lista = []
    a = requests.get(f"http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{pais}.xml")
    b = et.fromstring(a.content)
    xml = et.ElementTree(b)
    root = xml.getroot()
    for fact in root:
        if fact.find("GHO").text in indicadores:
            dicc = defaultdict(lambda: None)
            for child in fact:
                if child.tag in atributos:
                    if child.tag == "Numeric" or child.tag == "Low" or child.tag == "High":
                        dicc[child.tag] = float(child.text)
                    if child.tag == "Numeric":
                        if fact.find("GHO").text == "Mean systolic blood pressure (crude estimate)":
                            dicc["Presion arterial"] = float(child.text)
                        elif fact.find("GHO").text == "Mean fasting blood glucose (mmol/l) (crude estimate)":
                            dicc["Glucosa"] = float(child.text)
                        elif fact.find("GHO").text == "Mean Total Cholesterol (crude estimate)":
                            dicc["Colesterol"] = float(child.text)
                        elif fact.find("GHO").text == "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)":
                            dicc["Alcohol"] = float(child.text)
                        elif fact.find("GHO").text == "Estimate of daily cigarette smoking prevalence (%)":
                            dicc["Cigarros"] = float(child.text)
                        elif fact.find("GHO").text == "Estimate of daily tobacco smoking prevalence (%)":
                            dicc["Tabaco"] = float(child.text)
                    elif child.tag == "SEX":
                        if not child.text:
                            dicc[child.tag] = "Both sexes"
                        else:
                            dicc[child.tag] = child.text
                    elif child.tag == "YEAR":
                        dicc[child.tag] = date(year=int(child.text), month=1, day=1)
                    else:
                        dicc[child.tag] = child.text
            lista.append(dicc)
    for i in lista:
        lista = []
        for atr in atributos:
            lista.append(i[atr])
        dic[f"col{contador}"] = lista
        contador += 1

df = pd.DataFrame(data=dic).T
df = df.rename(columns={0: 'GHO', 1: 'YEAR', 2: 'Display', 3: 'COUNTRY', 4: 'SEX', 5: 'GHECAUSES', 6: 'AGEGROUP',
                        7: 'Numeric', 8: 'Low', 9: 'High', 10: 'Presión Arterial', 11: 'Glucosa', 12: 'Colesterol',
                        13: 'Alcohol', 14: 'Cigarro', 15: 'Tabaco'}, inplace=False)

gc = gspread.service_account(filename='molten-goal-316223-43641fcb1f3f.json')
sh = gc.open_by_key('1iPN6b104kkw1d88ZVfaPINmffhsWkAjNSrixm8FudDs')
worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc.

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, df) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET