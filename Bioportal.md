## APIs públicos del Departamento de Salud de Puerto Rico

Esta lista fue compilada por el Dr. Rafael Irizarry (@rafalab) y 
[el original está aquí](https://github.com/rafalab/pr-covid/blob/master/dashboard/apis.md)
(posiblemente más al día que esta copia).

* URL BASE: ```https://bioportal.salud.gov.pr/api```

* Método: HTTP GET

* Formato: application/json

## Endpoints
1. Cantidades totales de pruebas reportadas:
```/administration/reports/total```

2. Pruebas únicas con información mínima:
```/administration/reports/minimal-info-unique-tests```

3. Pruebas únicas con información mínima:
```/administration/reports/minimal-info```

4. Pruebas únicas con información mínima incluyendo fecha de entrada al Bioportal:
```/administration/reports/orders/basic```

5. Pruebas diarias para gráfica de dashboard de Salud:
```/administration/orders/dashboard-daily-testing```

6. Pruebas por fecha de colección:
```/administration/reports/tests-by-collected-date```

7. Pruebas por fecha de reporte:
```/administration/reports/tests-by-reported-date```

8. Pruebas por fecha de colección y entidad:
```/administration/reports/tests-by-collected-date-and-entity```

9. Total de TDF por fecha reportada de llegada:
```/administration/reports/travels/total-forms-by-reported-arrival-date```

10. Total de TDF por municipio:
```/administration/reports/travels/total-forms-by-municipalities```

11. Casos por fecha de colección:
```/administration/reports/cases/grouped-by-collected-date```

12. Casos por fecha de creación en sistema:
```/administration/reports/cases/dashboard-daily```

13. Casos por grupo de edad:
```/administration/reports/cases/dashboard-age-group```

14. Casos por ciudad:
```/administration/reports/cases/dashboard-city```

15. Casos por region:
```/administration/reports/cases/dashboard-region```
    
16. Resumen de Escuelas Públicas y Privadas:
```/administration/reports/education/general-summary```
    
17. Muertes por fecha de deceso:
```/administration/reports/deaths/summary```