# Airflow Database Ingestion Of Samples

This is the representation of an ingestion example where we will load a dataset into a dataabase using airflow.

## Requirements

* [Poetry](https://python-poetry.org/)

## Dataset

The dataset contains information of the TOP 50 songs from each month since the year 2000.

### Definition

| Column Name | Type | Is Null | Description |
| - | - | - | - |
| month | `VARCHAR(10)` | `False` | Stat's month and year period |
| position | `INTEGER` | `False` | Chart position |
| artist | `VARCHAR(100)` | `False` | Artist name |
| song | `VARCHAR(100)` | `False` | Song name |
| indicative_revenue | `NUMERIC` | `False` | Revenue generated by a song or album over a certain period |
| us | `INTEGER` | `False` | Song's peak position on US charts |
| uk | `INTEGER` | `False` | Song's peak position on UK charts |
| de | `INTEGER` | `False` | Song's peak position on DE charts |
| fr | `INTEGER` | `False` | Song's peak position on FR charts |
| ca | `INTEGER` | `False` | Song's peak position on CA charts |
| au | `INTEGER` | `False` | Song's peak position on AW charts |

### Disclaimer

Data has been extracted and parsed for ingestion to postgresql from [chart2000.com](https://chart2000.com/about.htm). 
