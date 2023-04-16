## Final Project Submission

Please fill out:
* Student name: Purity Murugi Riungu
* Student pace: part time 
* Scheduled project review date/time: 13/4/2023
* Instructor name: Noah Kandie
* Blog post URL:



Data Understanding.

In this project, i will work with various datasets which was collected from various locations.Some data are compressed CSV,TSV and IMDB.
The data to be used is contained in files below;
 1. im.db.zip
 2. bom.movie_gross.csv.gz

Business Understanding.

The Business Questions that will be tackled in the session will be;

1. What are the most successiful genres of movies?
2. What are the key characteristics of successiful movies? For instance the ethralling storyline, the Characters/cast.
3. How can Microsoft new movie studio position it self to be come the best?


```python
# Your code here - remember to use markdown cells for comments as well!

# import necessary libraries/data from the zipped files.

import csv
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
%matplotlib inline
```

The first step is to import all the necessary libraries.
for instance;
Pandas as pd,
numpy as np,
import csv,
matplotlib.pyplot as plt

import pandas as pd
import numpy as np

Data Preparation

The Next step is to read the data from bom.movie_gross.csv.gz


```python
# using pandas to load the csv file

import csv

bom_df = pd.read_csv('zippedData/bom.movie_gross.csv.gz')

bom_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>studio</th>
      <th>domestic_gross</th>
      <th>foreign_gross</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Toy Story 3</td>
      <td>BV</td>
      <td>415000000.0</td>
      <td>652000000</td>
      <td>2010</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Alice in Wonderland (2010)</td>
      <td>BV</td>
      <td>334200000.0</td>
      <td>691300000</td>
      <td>2010</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Harry Potter and the Deathly Hallows Part 1</td>
      <td>WB</td>
      <td>296000000.0</td>
      <td>664300000</td>
      <td>2010</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Inception</td>
      <td>WB</td>
      <td>292600000.0</td>
      <td>535700000</td>
      <td>2010</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Shrek Forever After</td>
      <td>P/DW</td>
      <td>238700000.0</td>
      <td>513900000</td>
      <td>2010</td>
    </tr>
  </tbody>
</table>
</div>




```python
#preview the data

bom_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 3387 entries, 0 to 3386
    Data columns (total 5 columns):
     #   Column          Non-Null Count  Dtype  
    ---  ------          --------------  -----  
     0   title           3387 non-null   object 
     1   studio          3382 non-null   object 
     2   domestic_gross  3359 non-null   float64
     3   foreign_gross   2037 non-null   object 
     4   year            3387 non-null   int64  
    dtypes: float64(1), int64(1), object(3)
    memory usage: 132.4+ KB
    


```python
bom_df.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>domestic_gross</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>3.359000e+03</td>
      <td>3387.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>2.874585e+07</td>
      <td>2013.958075</td>
    </tr>
    <tr>
      <th>std</th>
      <td>6.698250e+07</td>
      <td>2.478141</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000e+02</td>
      <td>2010.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>1.200000e+05</td>
      <td>2012.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1.400000e+06</td>
      <td>2014.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2.790000e+07</td>
      <td>2016.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>9.367000e+08</td>
      <td>2018.000000</td>
    </tr>
  </tbody>
</table>
</div>



Read the data of other files


```python
import csv

tn_df = pd.read_csv('zippedData/tn.movie_budgets.csv.gz')

tn_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>release_date</th>
      <th>movie</th>
      <th>production_budget</th>
      <th>domestic_gross</th>
      <th>worldwide_gross</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Dec 18, 2009</td>
      <td>Avatar</td>
      <td>$425,000,000</td>
      <td>$760,507,625</td>
      <td>$2,776,345,279</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>May 20, 2011</td>
      <td>Pirates of the Caribbean: On Stranger Tides</td>
      <td>$410,600,000</td>
      <td>$241,063,875</td>
      <td>$1,045,663,875</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Jun 7, 2019</td>
      <td>Dark Phoenix</td>
      <td>$350,000,000</td>
      <td>$42,762,350</td>
      <td>$149,762,350</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>May 1, 2015</td>
      <td>Avengers: Age of Ultron</td>
      <td>$330,600,000</td>
      <td>$459,005,868</td>
      <td>$1,403,013,963</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Dec 15, 2017</td>
      <td>Star Wars Ep. VIII: The Last Jedi</td>
      <td>$317,000,000</td>
      <td>$620,181,382</td>
      <td>$1,316,721,747</td>
    </tr>
  </tbody>
</table>
</div>




```python
#preview the data in tn.movie

tn_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 5782 entries, 0 to 5781
    Data columns (total 6 columns):
     #   Column             Non-Null Count  Dtype 
    ---  ------             --------------  ----- 
     0   id                 5782 non-null   int64 
     1   release_date       5782 non-null   object
     2   movie              5782 non-null   object
     3   production_budget  5782 non-null   object
     4   domestic_gross     5782 non-null   object
     5   worldwide_gross    5782 non-null   object
    dtypes: int64(1), object(5)
    memory usage: 271.2+ KB
    


```python
tn_df.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>5782.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>50.372363</td>
    </tr>
    <tr>
      <th>std</th>
      <td>28.821076</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>25.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>50.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>75.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>100.000000</td>
    </tr>
  </tbody>
</table>
</div>



Data analysis and data cleaning
To clean the Data in tn_df dataset
Removing special characters.


```python
# Define regular expression pattern to match special characters
pattern = r'[^a-zA-Z0-9\s]'

# Define function to clean text
def clean_text(text):
    text = re.sub(pattern, '', text)  # remove special characters
    text = re.sub(r'\s+', ' ', text)  # remove extra spaces
    text = text.title()  # convert to lowercase
    text = text.strip()  # remove leading and trailing spaces
    return text
```


```python
tn_df['production_budget'] = tn_df['production_budget'].str.replace('$', '').str.replace(',', '').astype(float)
tn_df['domestic_gross'] = tn_df['domestic_gross'].str.replace('$', '').str.replace(',', '').astype(float)
tn_df['worldwide_gross'] = tn_df['worldwide_gross'].str.replace('$', '').str.replace(',', '').astype(float)

```

    C:\Users\User\AppData\Local\Temp\ipykernel_12160\2912545500.py:1: FutureWarning: The default value of regex will change from True to False in a future version. In addition, single character regular expressions will *not* be treated as literal strings when regex=True.
      tn_df['production_budget'] = tn_df['production_budget'].str.replace('$', '').str.replace(',', '').astype(float)
    C:\Users\User\AppData\Local\Temp\ipykernel_12160\2912545500.py:2: FutureWarning: The default value of regex will change from True to False in a future version. In addition, single character regular expressions will *not* be treated as literal strings when regex=True.
      tn_df['domestic_gross'] = tn_df['domestic_gross'].str.replace('$', '').str.replace(',', '').astype(float)
    C:\Users\User\AppData\Local\Temp\ipykernel_12160\2912545500.py:3: FutureWarning: The default value of regex will change from True to False in a future version. In addition, single character regular expressions will *not* be treated as literal strings when regex=True.
      tn_df['worldwide_gross'] = tn_df['worldwide_gross'].str.replace('$', '').str.replace(',', '').astype(float)
    


```python
tn_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>release_date</th>
      <th>movie</th>
      <th>production_budget</th>
      <th>domestic_gross</th>
      <th>worldwide_gross</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Dec 18, 2009</td>
      <td>Avatar</td>
      <td>425000000.0</td>
      <td>760507625.0</td>
      <td>2.776345e+09</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>May 20, 2011</td>
      <td>Pirates of the Caribbean: On Stranger Tides</td>
      <td>410600000.0</td>
      <td>241063875.0</td>
      <td>1.045664e+09</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Jun 7, 2019</td>
      <td>Dark Phoenix</td>
      <td>350000000.0</td>
      <td>42762350.0</td>
      <td>1.497624e+08</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>May 1, 2015</td>
      <td>Avengers: Age of Ultron</td>
      <td>330600000.0</td>
      <td>459005868.0</td>
      <td>1.403014e+09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Dec 15, 2017</td>
      <td>Star Wars Ep. VIII: The Last Jedi</td>
      <td>317000000.0</td>
      <td>620181382.0</td>
      <td>1.316722e+09</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>5777</th>
      <td>78</td>
      <td>Dec 31, 2018</td>
      <td>Red 11</td>
      <td>7000.0</td>
      <td>0.0</td>
      <td>0.000000e+00</td>
    </tr>
    <tr>
      <th>5778</th>
      <td>79</td>
      <td>Apr 2, 1999</td>
      <td>Following</td>
      <td>6000.0</td>
      <td>48482.0</td>
      <td>2.404950e+05</td>
    </tr>
    <tr>
      <th>5779</th>
      <td>80</td>
      <td>Jul 13, 2005</td>
      <td>Return to the Land of Wonders</td>
      <td>5000.0</td>
      <td>1338.0</td>
      <td>1.338000e+03</td>
    </tr>
    <tr>
      <th>5780</th>
      <td>81</td>
      <td>Sep 29, 2015</td>
      <td>A Plague So Pleasant</td>
      <td>1400.0</td>
      <td>0.0</td>
      <td>0.000000e+00</td>
    </tr>
    <tr>
      <th>5781</th>
      <td>82</td>
      <td>Aug 5, 2005</td>
      <td>My Date With Drew</td>
      <td>1100.0</td>
      <td>181041.0</td>
      <td>1.810410e+05</td>
    </tr>
  </tbody>
</table>
<p>5782 rows × 6 columns</p>
</div>




```python
import csv

rt_df = pd.read_csv('zippedData/rt.movie_info.tsv.gz',sep='\t')

rt_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>synopsis</th>
      <th>rating</th>
      <th>genre</th>
      <th>director</th>
      <th>writer</th>
      <th>theater_date</th>
      <th>dvd_date</th>
      <th>currency</th>
      <th>box_office</th>
      <th>runtime</th>
      <th>studio</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>This gritty, fast-paced, and innovative police...</td>
      <td>R</td>
      <td>Action and Adventure|Classics|Drama</td>
      <td>William Friedkin</td>
      <td>Ernest Tidyman</td>
      <td>Oct 9, 1971</td>
      <td>Sep 25, 2001</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>104 minutes</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>New York City, not-too-distant-future: Eric Pa...</td>
      <td>R</td>
      <td>Drama|Science Fiction and Fantasy</td>
      <td>David Cronenberg</td>
      <td>David Cronenberg|Don DeLillo</td>
      <td>Aug 17, 2012</td>
      <td>Jan 1, 2013</td>
      <td>$</td>
      <td>600,000</td>
      <td>108 minutes</td>
      <td>Entertainment One</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5</td>
      <td>Illeana Douglas delivers a superb performance ...</td>
      <td>R</td>
      <td>Drama|Musical and Performing Arts</td>
      <td>Allison Anders</td>
      <td>Allison Anders</td>
      <td>Sep 13, 1996</td>
      <td>Apr 18, 2000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>116 minutes</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>6</td>
      <td>Michael Douglas runs afoul of a treacherous su...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Barry Levinson</td>
      <td>Paul Attanasio|Michael Crichton</td>
      <td>Dec 9, 1994</td>
      <td>Aug 27, 1997</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>128 minutes</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>NaN</td>
      <td>NR</td>
      <td>Drama|Romance</td>
      <td>Rodney Bennett</td>
      <td>Giles Cooper</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>200 minutes</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



Next step is to read the data from all data files provided and review/analyse the data.


```python
#preview the data in tn.movie

rt_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 1560 entries, 0 to 1559
    Data columns (total 12 columns):
     #   Column        Non-Null Count  Dtype 
    ---  ------        --------------  ----- 
     0   id            1560 non-null   int64 
     1   synopsis      1498 non-null   object
     2   rating        1557 non-null   object
     3   genre         1552 non-null   object
     4   director      1361 non-null   object
     5   writer        1111 non-null   object
     6   theater_date  1201 non-null   object
     7   dvd_date      1201 non-null   object
     8   currency      340 non-null    object
     9   box_office    340 non-null    object
     10  runtime       1530 non-null   object
     11  studio        494 non-null    object
    dtypes: int64(1), object(11)
    memory usage: 146.4+ KB
    


```python
rt_df.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>1560.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>1007.303846</td>
    </tr>
    <tr>
      <th>std</th>
      <td>579.164527</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>504.750000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1007.500000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>1503.250000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2000.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
import csv

rt_df = pd.read_csv('zippedData/rt.reviews.tsv.gz',sep='\t',encoding = 'latin1')

rt_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>review</th>
      <th>rating</th>
      <th>fresh</th>
      <th>critic</th>
      <th>top_critic</th>
      <th>publisher</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3</td>
      <td>A distinctly gallows take on contemporary fina...</td>
      <td>3/5</td>
      <td>fresh</td>
      <td>PJ Nabarro</td>
      <td>0</td>
      <td>Patrick Nabarro</td>
      <td>November 10, 2018</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>It's an allegory in search of a meaning that n...</td>
      <td>NaN</td>
      <td>rotten</td>
      <td>Annalee Newitz</td>
      <td>0</td>
      <td>io9.com</td>
      <td>May 23, 2018</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>... life lived in a bubble in financial dealin...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>Sean Axmaker</td>
      <td>0</td>
      <td>Stream on Demand</td>
      <td>January 4, 2018</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Continuing along a line introduced in last yea...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>Daniel Kasman</td>
      <td>0</td>
      <td>MUBI</td>
      <td>November 16, 2017</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>... a perverse twist on neorealism...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>NaN</td>
      <td>0</td>
      <td>Cinema Scope</td>
      <td>October 12, 2017</td>
    </tr>
  </tbody>
</table>
</div>




```python
import csv

reviews_df = pd.read_csv('zippedData/rt.reviews.tsv.gz',sep='\t',encoding = 'latin1')

reviews_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>review</th>
      <th>rating</th>
      <th>fresh</th>
      <th>critic</th>
      <th>top_critic</th>
      <th>publisher</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3</td>
      <td>A distinctly gallows take on contemporary fina...</td>
      <td>3/5</td>
      <td>fresh</td>
      <td>PJ Nabarro</td>
      <td>0</td>
      <td>Patrick Nabarro</td>
      <td>November 10, 2018</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>It's an allegory in search of a meaning that n...</td>
      <td>NaN</td>
      <td>rotten</td>
      <td>Annalee Newitz</td>
      <td>0</td>
      <td>io9.com</td>
      <td>May 23, 2018</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>... life lived in a bubble in financial dealin...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>Sean Axmaker</td>
      <td>0</td>
      <td>Stream on Demand</td>
      <td>January 4, 2018</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Continuing along a line introduced in last yea...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>Daniel Kasman</td>
      <td>0</td>
      <td>MUBI</td>
      <td>November 16, 2017</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>... a perverse twist on neorealism...</td>
      <td>NaN</td>
      <td>fresh</td>
      <td>NaN</td>
      <td>0</td>
      <td>Cinema Scope</td>
      <td>October 12, 2017</td>
    </tr>
  </tbody>
</table>
</div>




```python
reviews_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 54432 entries, 0 to 54431
    Data columns (total 8 columns):
     #   Column      Non-Null Count  Dtype 
    ---  ------      --------------  ----- 
     0   id          54432 non-null  int64 
     1   review      48869 non-null  object
     2   rating      40915 non-null  object
     3   fresh       54432 non-null  object
     4   critic      51710 non-null  object
     5   top_critic  54432 non-null  int64 
     6   publisher   54123 non-null  object
     7   date        54432 non-null  object
    dtypes: int64(2), object(6)
    memory usage: 3.3+ MB
    


```python
reviews_df.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>top_critic</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>54432.000000</td>
      <td>54432.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>1045.706882</td>
      <td>0.240594</td>
    </tr>
    <tr>
      <th>std</th>
      <td>586.657046</td>
      <td>0.427448</td>
    </tr>
    <tr>
      <th>min</th>
      <td>3.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>542.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1083.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>1541.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2000.000000</td>
      <td>1.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
tmdb_df = pd.read_csv('zippedData/tmdb.movies.csv.gz')

tmdb_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>genre_ids</th>
      <th>id</th>
      <th>original_language</th>
      <th>original_title</th>
      <th>popularity</th>
      <th>release_date</th>
      <th>title</th>
      <th>vote_average</th>
      <th>vote_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>[12, 14, 10751]</td>
      <td>12444</td>
      <td>en</td>
      <td>Harry Potter and the Deathly Hallows: Part 1</td>
      <td>33.533</td>
      <td>2010-11-19</td>
      <td>Harry Potter and the Deathly Hallows: Part 1</td>
      <td>7.7</td>
      <td>10788</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>[14, 12, 16, 10751]</td>
      <td>10191</td>
      <td>en</td>
      <td>How to Train Your Dragon</td>
      <td>28.734</td>
      <td>2010-03-26</td>
      <td>How to Train Your Dragon</td>
      <td>7.7</td>
      <td>7610</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>[12, 28, 878]</td>
      <td>10138</td>
      <td>en</td>
      <td>Iron Man 2</td>
      <td>28.515</td>
      <td>2010-05-07</td>
      <td>Iron Man 2</td>
      <td>6.8</td>
      <td>12368</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>[16, 35, 10751]</td>
      <td>862</td>
      <td>en</td>
      <td>Toy Story</td>
      <td>28.005</td>
      <td>1995-11-22</td>
      <td>Toy Story</td>
      <td>7.9</td>
      <td>10174</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>[28, 878, 12]</td>
      <td>27205</td>
      <td>en</td>
      <td>Inception</td>
      <td>27.920</td>
      <td>2010-07-16</td>
      <td>Inception</td>
      <td>8.3</td>
      <td>22186</td>
    </tr>
  </tbody>
</table>
</div>




```python
tmdb_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 26517 entries, 0 to 26516
    Data columns (total 10 columns):
     #   Column             Non-Null Count  Dtype  
    ---  ------             --------------  -----  
     0   Unnamed: 0         26517 non-null  int64  
     1   genre_ids          26517 non-null  object 
     2   id                 26517 non-null  int64  
     3   original_language  26517 non-null  object 
     4   original_title     26517 non-null  object 
     5   popularity         26517 non-null  float64
     6   release_date       26517 non-null  object 
     7   title              26517 non-null  object 
     8   vote_average       26517 non-null  float64
     9   vote_count         26517 non-null  int64  
    dtypes: float64(2), int64(3), object(5)
    memory usage: 2.0+ MB
    


```python
tmdb_df.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>id</th>
      <th>popularity</th>
      <th>vote_average</th>
      <th>vote_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>26517.00000</td>
      <td>26517.000000</td>
      <td>26517.000000</td>
      <td>26517.000000</td>
      <td>26517.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>13258.00000</td>
      <td>295050.153260</td>
      <td>3.130912</td>
      <td>5.991281</td>
      <td>194.224837</td>
    </tr>
    <tr>
      <th>std</th>
      <td>7654.94288</td>
      <td>153661.615648</td>
      <td>4.355229</td>
      <td>1.852946</td>
      <td>960.961095</td>
    </tr>
    <tr>
      <th>min</th>
      <td>0.00000</td>
      <td>27.000000</td>
      <td>0.600000</td>
      <td>0.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>6629.00000</td>
      <td>157851.000000</td>
      <td>0.600000</td>
      <td>5.000000</td>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>13258.00000</td>
      <td>309581.000000</td>
      <td>1.374000</td>
      <td>6.000000</td>
      <td>5.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>19887.00000</td>
      <td>419542.000000</td>
      <td>3.694000</td>
      <td>7.000000</td>
      <td>28.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>26516.00000</td>
      <td>608444.000000</td>
      <td>80.773000</td>
      <td>10.000000</td>
      <td>22186.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
tmdb_df.isna().sum()
```




    Unnamed: 0           0
    genre_ids            0
    id                   0
    original_language    0
    original_title       0
    popularity           0
    release_date         0
    title                0
    vote_average         0
    vote_count           0
    dtype: int64



Data Cleaning
The next step will be to perform data cleaning.
1. Identify the missing values in the various data sets.



```python
#tn movie budgets

tn_df.isna()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>release_date</th>
      <th>movie</th>
      <th>production_budget</th>
      <th>domestic_gross</th>
      <th>worldwide_gross</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>5777</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>5778</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>5779</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>5780</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>5781</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>5782 rows × 6 columns</p>
</div>




```python
tn_df.isna().sum()
```




    id                   0
    release_date         0
    movie                0
    production_budget    0
    domestic_gross       0
    worldwide_gross      0
    dtype: int64




```python
reviews_df.isna()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>review</th>
      <th>rating</th>
      <th>fresh</th>
      <th>critic</th>
      <th>top_critic</th>
      <th>publisher</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>54427</th>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>54428</th>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>54429</th>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>54430</th>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>54431</th>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>54432 rows × 8 columns</p>
</div>




```python
reviews_df.isna().sum()
```




    id                0
    review         5563
    rating        13517
    fresh             0
    critic         2722
    top_critic        0
    publisher       309
    date              0
    dtype: int64




```python
rt_df.isna().sum()
```




    id                0
    review         5563
    rating        13517
    fresh             0
    critic         2722
    top_critic        0
    publisher       309
    date              0
    dtype: int64




```python
bom_df.isna()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>studio</th>
      <th>domestic_gross</th>
      <th>foreign_gross</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>3382</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3383</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3384</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3385</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3386</th>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>3387 rows × 5 columns</p>
</div>




```python
bom_df.isna().sum()
```




    title                0
    studio               5
    domestic_gross      28
    foreign_gross     1350
    year                 0
    dtype: int64



After identifying the missing Data, we examine and decide on how to deal with the missing data.


```python
 #check for missing percentages in bom_df 

missing_percentages =(bom_df.isna()/ len(bom_df))*100

print(missing_percentages)
```

          title  studio  domestic_gross  foreign_gross  year
    0       0.0     0.0             0.0       0.000000   0.0
    1       0.0     0.0             0.0       0.000000   0.0
    2       0.0     0.0             0.0       0.000000   0.0
    3       0.0     0.0             0.0       0.000000   0.0
    4       0.0     0.0             0.0       0.000000   0.0
    ...     ...     ...             ...            ...   ...
    3382    0.0     0.0             0.0       0.029525   0.0
    3383    0.0     0.0             0.0       0.029525   0.0
    3384    0.0     0.0             0.0       0.029525   0.0
    3385    0.0     0.0             0.0       0.029525   0.0
    3386    0.0     0.0             0.0       0.029525   0.0
    
    [3387 rows x 5 columns]
    


```python
#checking for unique values

bom_df['foreign_gross'].unique()

```




    array(['652000000', '691300000', '664300000', ..., '530000', '256000',
           '30000'], dtype=object)




```python
bom_df = bom_df.drop("foreign_gross", axis=1)

print(bom_df)
```

                                                title      studio  domestic_gross  \
    0                                     Toy Story 3          BV     415000000.0   
    1                      Alice in Wonderland (2010)          BV     334200000.0   
    2     Harry Potter and the Deathly Hallows Part 1          WB     296000000.0   
    3                                       Inception          WB     292600000.0   
    4                             Shrek Forever After        P/DW     238700000.0   
    ...                                           ...         ...             ...   
    3382                                    The Quake       Magn.          6200.0   
    3383                  Edward II (2018 re-release)          FM          4800.0   
    3384                                     El Pacto        Sony          2500.0   
    3385                                     The Swan  Synergetic          2400.0   
    3386                            An Actor Prepares       Grav.          1700.0   
    
          year  
    0     2010  
    1     2010  
    2     2010  
    3     2010  
    4     2010  
    ...    ...  
    3382  2018  
    3383  2018  
    3384  2018  
    3385  2018  
    3386  2018  
    
    [3387 rows x 4 columns]
    


```python
#checkng for duplicates values

duplicates =reviews_df[reviews_df.duplicated(subset= 'review')]

print(len(duplicates))

duplicates.head()
```

    5749
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>review</th>
      <th>rating</th>
      <th>fresh</th>
      <th>critic</th>
      <th>top_critic</th>
      <th>publisher</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>169</th>
      <td>5</td>
      <td>NaN</td>
      <td>3/5</td>
      <td>fresh</td>
      <td>Cole Smithey</td>
      <td>0</td>
      <td>ColeSmithey.com</td>
      <td>October 10, 2005</td>
    </tr>
    <tr>
      <th>170</th>
      <td>5</td>
      <td>NaN</td>
      <td>2/5</td>
      <td>rotten</td>
      <td>Chuck O'Leary</td>
      <td>0</td>
      <td>Fantastica Daily</td>
      <td>October 9, 2005</td>
    </tr>
    <tr>
      <th>171</th>
      <td>5</td>
      <td>NaN</td>
      <td>3/5</td>
      <td>fresh</td>
      <td>Philip Martin</td>
      <td>0</td>
      <td>Arkansas Democrat-Gazette</td>
      <td>April 28, 2005</td>
    </tr>
    <tr>
      <th>172</th>
      <td>5</td>
      <td>NaN</td>
      <td>3/5</td>
      <td>fresh</td>
      <td>Eric Melin</td>
      <td>0</td>
      <td>Lawrence.com</td>
      <td>August 24, 2004</td>
    </tr>
    <tr>
      <th>173</th>
      <td>5</td>
      <td>NaN</td>
      <td>6/10</td>
      <td>fresh</td>
      <td>Dragan Antulov</td>
      <td>0</td>
      <td>rec.arts.movies.reviews</td>
      <td>November 5, 2003</td>
    </tr>
  </tbody>
</table>
</div>



To Answer the first question, 

I will look at earnings of different movies
I will check the review ratings in different movies


```python
import csv

rt_df = pd.read_csv('zippedData/rt.movie_info.tsv.gz',sep='\t')

genre_df = rt_df[['genre']]

print(rt_df.columns)
```

    Index(['id', 'synopsis', 'rating', 'genre', 'director', 'writer',
           'theater_date', 'dvd_date', 'currency', 'box_office', 'runtime',
           'studio'],
          dtype='object')
    


```python
print(rt_df.shape)
```

    (1560, 12)
    


```python
print(genre_df.head())
```

                                     genre
    0  Action and Adventure|Classics|Drama
    1    Drama|Science Fiction and Fantasy
    2    Drama|Musical and Performing Arts
    3           Drama|Mystery and Suspense
    4                        Drama|Romance
    


```python
import csv

rt_df = pd.read_csv('zippedData/rt.movie_info.tsv.gz',sep='\t')

genre_rating_df = rt_df[['genre','rating']]

print(rt_df.columns)
```

    Index(['id', 'synopsis', 'rating', 'genre', 'director', 'writer',
           'theater_date', 'dvd_date', 'currency', 'box_office', 'runtime',
           'studio'],
          dtype='object')
    


```python
print(genre_rating_df.head())
```

                                     genre rating
    0  Action and Adventure|Classics|Drama      R
    1    Drama|Science Fiction and Fantasy      R
    2    Drama|Musical and Performing Arts      R
    3           Drama|Mystery and Suspense      R
    4                        Drama|Romance     NR
    

Rename the movie to itle name to have wo smilar columns to merge.

Joining of two datasets with a unique column:


```python
tn_df = tn_df.rename(columns={'movie': 'title'})
```


```python
tn_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>release_date</th>
      <th>title</th>
      <th>production_budget</th>
      <th>domestic_gross</th>
      <th>worldwide_gross</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Dec 18, 2009</td>
      <td>Avatar</td>
      <td>425000000.0</td>
      <td>760507625.0</td>
      <td>2.776345e+09</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>May 20, 2011</td>
      <td>Pirates of the Caribbean: On Stranger Tides</td>
      <td>410600000.0</td>
      <td>241063875.0</td>
      <td>1.045664e+09</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Jun 7, 2019</td>
      <td>Dark Phoenix</td>
      <td>350000000.0</td>
      <td>42762350.0</td>
      <td>1.497624e+08</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>May 1, 2015</td>
      <td>Avengers: Age of Ultron</td>
      <td>330600000.0</td>
      <td>459005868.0</td>
      <td>1.403014e+09</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Dec 15, 2017</td>
      <td>Star Wars Ep. VIII: The Last Jedi</td>
      <td>317000000.0</td>
      <td>620181382.0</td>
      <td>1.316722e+09</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>5777</th>
      <td>78</td>
      <td>Dec 31, 2018</td>
      <td>Red 11</td>
      <td>7000.0</td>
      <td>0.0</td>
      <td>0.000000e+00</td>
    </tr>
    <tr>
      <th>5778</th>
      <td>79</td>
      <td>Apr 2, 1999</td>
      <td>Following</td>
      <td>6000.0</td>
      <td>48482.0</td>
      <td>2.404950e+05</td>
    </tr>
    <tr>
      <th>5779</th>
      <td>80</td>
      <td>Jul 13, 2005</td>
      <td>Return to the Land of Wonders</td>
      <td>5000.0</td>
      <td>1338.0</td>
      <td>1.338000e+03</td>
    </tr>
    <tr>
      <th>5780</th>
      <td>81</td>
      <td>Sep 29, 2015</td>
      <td>A Plague So Pleasant</td>
      <td>1400.0</td>
      <td>0.0</td>
      <td>0.000000e+00</td>
    </tr>
    <tr>
      <th>5781</th>
      <td>82</td>
      <td>Aug 5, 2005</td>
      <td>My Date With Drew</td>
      <td>1100.0</td>
      <td>181041.0</td>
      <td>1.810410e+05</td>
    </tr>
  </tbody>
</table>
<p>5782 rows × 6 columns</p>
</div>




```python
merged_bom_tn_df = pd.merge(bom_df,tn_df,on='title')
merged_bom_tn_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>studio</th>
      <th>domestic_gross_x</th>
      <th>year</th>
      <th>id</th>
      <th>release_date</th>
      <th>production_budget</th>
      <th>domestic_gross_y</th>
      <th>worldwide_gross</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Toy Story 3</td>
      <td>BV</td>
      <td>415000000.0</td>
      <td>2010</td>
      <td>47</td>
      <td>Jun 18, 2010</td>
      <td>200000000.0</td>
      <td>415004880.0</td>
      <td>1.068880e+09</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Inception</td>
      <td>WB</td>
      <td>292600000.0</td>
      <td>2010</td>
      <td>38</td>
      <td>Jul 16, 2010</td>
      <td>160000000.0</td>
      <td>292576195.0</td>
      <td>8.355246e+08</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Shrek Forever After</td>
      <td>P/DW</td>
      <td>238700000.0</td>
      <td>2010</td>
      <td>27</td>
      <td>May 21, 2010</td>
      <td>165000000.0</td>
      <td>238736787.0</td>
      <td>7.562447e+08</td>
    </tr>
    <tr>
      <th>3</th>
      <td>The Twilight Saga: Eclipse</td>
      <td>Sum.</td>
      <td>300500000.0</td>
      <td>2010</td>
      <td>53</td>
      <td>Jun 30, 2010</td>
      <td>68000000.0</td>
      <td>300531751.0</td>
      <td>7.061028e+08</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Iron Man 2</td>
      <td>Par.</td>
      <td>312400000.0</td>
      <td>2010</td>
      <td>15</td>
      <td>May 7, 2010</td>
      <td>170000000.0</td>
      <td>312433331.0</td>
      <td>6.211564e+08</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1242</th>
      <td>Gotti</td>
      <td>VE</td>
      <td>4300000.0</td>
      <td>2018</td>
      <td>64</td>
      <td>Jun 15, 2018</td>
      <td>10000000.0</td>
      <td>4286367.0</td>
      <td>6.089100e+06</td>
    </tr>
    <tr>
      <th>1243</th>
      <td>Ben is Back</td>
      <td>RAtt.</td>
      <td>3700000.0</td>
      <td>2018</td>
      <td>95</td>
      <td>Dec 7, 2018</td>
      <td>13000000.0</td>
      <td>3703182.0</td>
      <td>9.633111e+06</td>
    </tr>
    <tr>
      <th>1244</th>
      <td>Bilal: A New Breed of Hero</td>
      <td>VE</td>
      <td>491000.0</td>
      <td>2018</td>
      <td>100</td>
      <td>Feb 2, 2018</td>
      <td>30000000.0</td>
      <td>490973.0</td>
      <td>6.485990e+05</td>
    </tr>
    <tr>
      <th>1245</th>
      <td>Mandy</td>
      <td>RLJ</td>
      <td>1200000.0</td>
      <td>2018</td>
      <td>71</td>
      <td>Sep 14, 2018</td>
      <td>6000000.0</td>
      <td>1214525.0</td>
      <td>1.427656e+06</td>
    </tr>
    <tr>
      <th>1246</th>
      <td>Lean on Pete</td>
      <td>A24</td>
      <td>1200000.0</td>
      <td>2018</td>
      <td>13</td>
      <td>Apr 6, 2018</td>
      <td>8000000.0</td>
      <td>1163056.0</td>
      <td>2.455027e+06</td>
    </tr>
  </tbody>
</table>
<p>1247 rows × 9 columns</p>
</div>




```python
merged_bom_tn_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 1247 entries, 0 to 1246
    Data columns (total 9 columns):
     #   Column             Non-Null Count  Dtype  
    ---  ------             --------------  -----  
     0   title              1247 non-null   object 
     1   studio             1246 non-null   object 
     2   domestic_gross_x   1245 non-null   float64
     3   year               1247 non-null   int64  
     4   id                 1247 non-null   int64  
     5   release_date       1247 non-null   object 
     6   production_budget  1247 non-null   float64
     7   domestic_gross_y   1247 non-null   float64
     8   worldwide_gross    1247 non-null   float64
    dtypes: float64(4), int64(2), object(3)
    memory usage: 97.4+ KB
    


```python
df_profit = merged_bom_tn_df['worldwide_gross'] - merged_bom_tn_df['production_budget']

print(df_profit)
```

    0       868879522.0
    1       675524642.0
    2       591244673.0
    3       638102828.0
    4       451156389.0
               ...     
    1242     -3910900.0
    1243     -3366889.0
    1244    -29351401.0
    1245     -4572344.0
    1246     -5544973.0
    Length: 1247, dtype: float64
    

Data Visualisation

#Visualisation of the Worldwide_gross and Production Budget.


```python
import matplotlib.pyplot as plt

# Create a scatter plot of production_budget vs. worldwide_gross
plt.scatter(tn_df['production_budget'], tn_df['worldwide_gross'])

# Set the x-axis and y-axis labels
plt.xlabel('Production Budget ($)')
plt.ylabel('Worldwide Gross Revenue ($)')

# Set the plot title
plt.title('Production Budget vs. Worldwide Gross Revenue')

# Show the plot
plt.show()
```


    
![png](output_56_0.png)
    


Now lets look at the Cost verses the revenue of different movies.

Plot a graph between domestic and wordwide gross



```python
import matplotlib.pyplot as plt

# Create a scatter plot of domestic_gross_y vs. worldwide_gross
plt.scatter(merged_bom_tn_df['domestic_gross_y'], merged_bom_tn_df['worldwide_gross'])

# Set the x-axis and y-axis labels
plt.xlabel('domestic gross ($)')
plt.ylabel('Worldwide Gross Revenue ($)')

# Set the plot title
plt.title('domestic gross vs. Worldwide Gross Revenue')

# Show the plot
plt.show()
```


    
![png](output_58_0.png)
    



```python
import seaborn as sns

# Create a scatter plot of domestic and worldwide gross revenue for each movie
sns.scatterplot(x='domestic_gross_x', y='worldwide_gross', data=merged_bom_tn_df)

# Set the axis labels and title
plt.xlabel('Domestic Gross Revenue ($)')
plt.ylabel('Worldwide Gross Revenue ($)')
plt.title('Domestic vs. Worldwide Gross Revenue')

# Show the plot
plt.show()

```


    
![png](output_59_0.png)
    



```python
print(tmdb_df.columns)
```

    Index(['Unnamed: 0', 'genre_ids', 'id', 'original_language', 'original_title',
           'popularity', 'release_date', 'title', 'vote_average', 'vote_count'],
          dtype='object')
    


```python
#join the tmdb data and bom movie data

merged_bom_tmdb_df = pd.merge(bom_df,tmdb_df,on='title')

merged_bom_tmdb_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>studio</th>
      <th>domestic_gross</th>
      <th>year</th>
      <th>Unnamed: 0</th>
      <th>genre_ids</th>
      <th>id</th>
      <th>original_language</th>
      <th>original_title</th>
      <th>popularity</th>
      <th>release_date</th>
      <th>vote_average</th>
      <th>vote_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Toy Story 3</td>
      <td>BV</td>
      <td>415000000.0</td>
      <td>2010</td>
      <td>7</td>
      <td>[16, 10751, 35]</td>
      <td>10193</td>
      <td>en</td>
      <td>Toy Story 3</td>
      <td>24.445</td>
      <td>2010-06-17</td>
      <td>7.7</td>
      <td>8340</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Inception</td>
      <td>WB</td>
      <td>292600000.0</td>
      <td>2010</td>
      <td>4</td>
      <td>[28, 878, 12]</td>
      <td>27205</td>
      <td>en</td>
      <td>Inception</td>
      <td>27.920</td>
      <td>2010-07-16</td>
      <td>8.3</td>
      <td>22186</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Shrek Forever After</td>
      <td>P/DW</td>
      <td>238700000.0</td>
      <td>2010</td>
      <td>38</td>
      <td>[35, 12, 14, 16, 10751]</td>
      <td>10192</td>
      <td>en</td>
      <td>Shrek Forever After</td>
      <td>15.041</td>
      <td>2010-05-16</td>
      <td>6.1</td>
      <td>3843</td>
    </tr>
    <tr>
      <th>3</th>
      <td>The Twilight Saga: Eclipse</td>
      <td>Sum.</td>
      <td>300500000.0</td>
      <td>2010</td>
      <td>15</td>
      <td>[12, 14, 18, 10749]</td>
      <td>24021</td>
      <td>en</td>
      <td>The Twilight Saga: Eclipse</td>
      <td>20.340</td>
      <td>2010-06-23</td>
      <td>6.0</td>
      <td>4909</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Iron Man 2</td>
      <td>Par.</td>
      <td>312400000.0</td>
      <td>2010</td>
      <td>2</td>
      <td>[12, 28, 878]</td>
      <td>10138</td>
      <td>en</td>
      <td>Iron Man 2</td>
      <td>28.515</td>
      <td>2010-05-07</td>
      <td>6.8</td>
      <td>12368</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2698</th>
      <td>The Escape</td>
      <td>IFC</td>
      <td>14000.0</td>
      <td>2018</td>
      <td>16803</td>
      <td>[53, 28]</td>
      <td>459814</td>
      <td>en</td>
      <td>The Escape</td>
      <td>0.600</td>
      <td>2015-08-14</td>
      <td>7.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2699</th>
      <td>The Escape</td>
      <td>IFC</td>
      <td>14000.0</td>
      <td>2018</td>
      <td>19053</td>
      <td>[53, 28]</td>
      <td>417004</td>
      <td>en</td>
      <td>The Escape</td>
      <td>1.176</td>
      <td>2016-10-23</td>
      <td>6.6</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2700</th>
      <td>Souvenir</td>
      <td>Strand</td>
      <td>11400.0</td>
      <td>2018</td>
      <td>18483</td>
      <td>[35, 18]</td>
      <td>408258</td>
      <td>fr</td>
      <td>Souvenir</td>
      <td>2.130</td>
      <td>2016-09-08</td>
      <td>5.8</td>
      <td>14</td>
    </tr>
    <tr>
      <th>2701</th>
      <td>The Quake</td>
      <td>Magn.</td>
      <td>6200.0</td>
      <td>2018</td>
      <td>24107</td>
      <td>[12]</td>
      <td>416194</td>
      <td>no</td>
      <td>Skjelvet</td>
      <td>11.051</td>
      <td>2018-12-14</td>
      <td>6.7</td>
      <td>81</td>
    </tr>
    <tr>
      <th>2702</th>
      <td>An Actor Prepares</td>
      <td>Grav.</td>
      <td>1700.0</td>
      <td>2018</td>
      <td>24419</td>
      <td>[35, 18]</td>
      <td>434596</td>
      <td>en</td>
      <td>An Actor Prepares</td>
      <td>7.244</td>
      <td>2018-08-31</td>
      <td>6.5</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
<p>2703 rows × 13 columns</p>
</div>




```python

```


```python
import csv

tmdb_df = pd.read_csv('zippedData/tmdb.movies.csv.gz')

title_vote_df = tmdb_df[['title','vote_average']]

print(tmdb_df.columns)
```

    Index(['Unnamed: 0', 'genre_ids', 'id', 'original_language', 'original_title',
           'popularity', 'release_date', 'title', 'vote_average', 'vote_count'],
          dtype='object')
    


```python
print(title_vote_df.head())
```


```python
import pandas as pd
import matplotlib.pyplot as plt

# Create a data frame
data = {
    'title': ['Harry Potter and the Deathly Hallows: Part 1', 'How to Train Your Dragon', 'Iron Man 2', 'Toy Story', 'Inception'],
    'vote_average': [7.7, 7.7, 6.8, 7.9, 8.3]
}
df = pd.DataFrame(data)

# Create a bar plot
df.plot(kind='bar', x='title', y='vote_average')

# Set the plot title and axis labels
plt.title('Movie Ratings')
plt.xlabel('Title')
plt.ylabel('Vote Average')

# Show the plot
plt.show()
```


    
![png](output_65_0.png)
    



```python
import csv

bom_df = pd.read_csv('zippedData/bom.movie_gross.csv.gz')

income_df = bom_df[['title','foreign_gross']]

print(income_df.columns)
```

    Index(['title', 'foreign_gross'], dtype='object')
    


```python
print(income_df.head())
```

                                             title foreign_gross
    0                                  Toy Story 3     652000000
    1                   Alice in Wonderland (2010)     691300000
    2  Harry Potter and the Deathly Hallows Part 1     664300000
    3                                    Inception     535700000
    4                          Shrek Forever After     513900000
    


```python
import pandas as pd
import matplotlib.pyplot as plt

# Create a data frame
data = {
    'title': ['Toy Story 3', 'Alice in Wonderland (2010)', 'Harry Potter and the Deathly Hallows Part 1', 'Inception', 'Shrek Forever After'],
    'foreign_gross': [652000000, 691300000, 664300000, 535700000, 513900000]
}
df = pd.DataFrame(data)

# Create a bar plot
df.plot(kind='bar', x='title', y='foreign_gross')

# Set the plot title and axis labels
plt.title('Gross Revenue Per Title')
plt.xlabel('Movie Title')
plt.ylabel('Foreign Gross Revenue (in millions)')

# Show the plot
plt.show()

```


    
![png](output_68_0.png)
    


From above workings, it clearly shows that there is no direct relationship between the movie with the highest viewing on average and the gross income.

Tackling Question 2

What are the key characteristics of successiful movies? For instance the ethralling storyline, the Characters/cast.

I will consider the audience reactions,and feedback for different genres and box_office i.e earnings from sales of different genres.


```python
import csv

rt_df = pd.read_csv('zippedData/rt.movie_info.tsv.gz',sep='\t')

#creating a new data frame of genres and box_office.

genre_box_df = rt_df[['genre','box_office']]

print(genre_box_df.columns)
```

    Index(['genre', 'box_office'], dtype='object')
    


```python
print(genre_box_df.head(20))
```

                                                    genre  box_office
    0                 Action and Adventure|Classics|Drama         NaN
    1                   Drama|Science Fiction and Fantasy     600,000
    2                   Drama|Musical and Performing Arts         NaN
    3                          Drama|Mystery and Suspense         NaN
    4                                       Drama|Romance         NaN
    5                               Drama|Kids and Family         NaN
    6                                              Comedy  41,032,915
    7                                               Drama     224,114
    8                                               Drama     134,904
    9   Action and Adventure|Mystery and Suspense|Scie...         NaN
    10                                                NaN         NaN
    11                                        Documentary         NaN
    12                       Documentary|Special Interest         NaN
    13                              Classics|Comedy|Drama         NaN
    14                                             Comedy         NaN
    15                  Comedy|Drama|Mystery and Suspense   1,039,869
    16                                              Drama  99,165,609
    17                  Action and Adventure|Comedy|Drama         NaN
    18  Action and Adventure|Drama|Science Fiction and...  20,518,224
    19  Art House and International|Comedy|Drama|Music...   1,971,135
    


```python
#cleaning the new dataframe created

genre_box_df.dropna(subset=['box_office'],inplace=True)
```

    C:\Users\User\AppData\Local\Temp\ipykernel_12160\3940971107.py:3: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      genre_box_df.dropna(subset=['box_office'],inplace=True)
    


```python
print(genre_box_df)
```

                                                     genre  box_office
    1                    Drama|Science Fiction and Fantasy     600,000
    6                                               Comedy  41,032,915
    7                                                Drama     224,114
    8                                                Drama     134,904
    15                   Comedy|Drama|Mystery and Suspense   1,039,869
    ...                                                ...         ...
    1541  Action and Adventure|Science Fiction and Fantasy  25,335,935
    1542                                      Comedy|Drama   1,416,189
    1545                       Horror|Mystery and Suspense      59,371
    1546          Art House and International|Comedy|Drama     794,306
    1555  Action and Adventure|Horror|Mystery and Suspense  33,886,034
    
    [340 rows x 2 columns]
    


```python
import pandas as pd
import matplotlib.pyplot as plt

# create a dataframe from the given data
genre_box_df= {'genre': ['Drama|Science Fiction and Fantasy', 'Comedy', 'Drama', 'Drama', 'Comedy|Drama|Mystery and Suspense', 'Action and Adventure|Science Fiction and Fantasy', 'Comedy|Drama', 'Horror|Mystery and Suspense', 'Art House and International|Comedy|Drama', 'Action and Adventure|Horror|Mystery and Suspense'],
        'box_office': [600000, 41032915, 224114, 134904, 1039869, 25335935, 1416189, 59371, 794306, 33886034]}
df = pd.DataFrame(data)

```


```python
import pandas as pd
import matplotlib.pyplot as plt

# create a dataframe from the given data
data = {'genre': ['Drama|Science Fiction and Fantasy', 'Comedy', 'Drama', 'Drama', 'Comedy|Drama|Mystery and Suspense', 'Action and Adventure|Science Fiction and Fantasy', 'Comedy|Drama', 'Horror|Mystery and Suspense', 'Art House and International|Comedy|Drama', 'Action and Adventure|Horror|Mystery and Suspense'],
        'box_office': [600000, 41032915, 224114, 134904, 1039869, 25335935, 1416189, 59371, 794306, 33886034]}
df = pd.DataFrame(data)

# split genre strings into separate genres
df['genre'] = df['genre'].str.split('|')

# create a separate row for each genre
df = df.explode('genre')

# group by genre and sum the box office revenue
grouped = df.groupby('genre')['box_office'].sum()

# create a bar graph
fig, ax = plt.subplots(figsize=(12, 8))
ax.bar(grouped.index, grouped.values)
ax.set_title('Total Box Office Revenue by Genre')
ax.set_xlabel('Genre')
ax.set_ylabel('Box Office Revenue')
plt.xticks(rotation=90)
plt.show()
```


    
![png](output_76_0.png)
    


from above, it shows that the action and adventure genres have more sales, followed closely by Commedy. This is from a sample of the first 20 genres in the provided data set.Therefore microsoft should look forward into investing in Action related genres and Commedy.

Tackling Question 3
How can Microsoft new movie studio position it self to be come the best?

#Microsoft can position itself by consideriung the perfomance of previous genres and subsequent revenue to decide on the best.
Consider the performance of various directors as per the movies created


```python
import pandas as pd
import matplotlib.pyplot as plt

# create a dataframe from the given data
data = {'genre': ['Drama|Science Fiction and Fantasy', 'Comedy', 'Drama', 'Drama', 'Comedy|Drama|Mystery and Suspense', 'Action and Adventure|Science Fiction and Fantasy', 'Comedy|Drama', 'Horror|Mystery and Suspense', 'Art House and International|Comedy|Drama', 'Action and Adventure|Horror|Mystery and Suspense'],
        'box_office': [600000, 41032915, 224114, 134904, 1039869, 25335935, 1416189, 59371, 794306, 33886034]}
df = pd.DataFrame(data)

# split genre strings into separate genres
df['genre'] = df['genre'].str.split('|')

# create a separate row for each genre
df = df.explode('genre')

# group by genre and sum the box office revenue
grouped = df.groupby('genre')['box_office'].sum()

# create a bar graph
fig, ax = plt.subplots(figsize=(12, 8))
ax.bar(grouped.index, grouped.values)
ax.set_title('Total Box Office Revenue by Genre')
ax.set_xlabel('Genre')
ax.set_ylabel('Box Office Revenue')
plt.xticks(rotation=90)
plt.show()
```


    
![png](output_80_0.png)
    


#merge bom_df and rt_df


```python
merged_bom_rt_df = pd.merge(bom_df,rt_df,on='studio')

merged_bom_rt_df.head(30)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>studio</th>
      <th>domestic_gross</th>
      <th>year</th>
      <th>id</th>
      <th>synopsis</th>
      <th>rating</th>
      <th>genre</th>
      <th>director</th>
      <th>writer</th>
      <th>theater_date</th>
      <th>dvd_date</th>
      <th>currency</th>
      <th>box_office</th>
      <th>runtime</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Harry Potter and the Deathly Hallows Part 1</td>
      <td>WB</td>
      <td>296000000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Inception</td>
      <td>WB</td>
      <td>292600000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Clash of the Titans (2010)</td>
      <td>WB</td>
      <td>163200000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Due Date</td>
      <td>WB</td>
      <td>100500000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Yogi Bear</td>
      <td>WB</td>
      <td>100200000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>5</th>
      <td>The Book of Eli</td>
      <td>WB</td>
      <td>94800000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>6</th>
      <td>The Town</td>
      <td>WB</td>
      <td>92200000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Legend of the Guardians: The Owls of Ga'Hoole</td>
      <td>WB</td>
      <td>55700000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Cats &amp; Dogs: The Revenge of Kitty Galore</td>
      <td>WB</td>
      <td>43600000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Life as We Know It</td>
      <td>WB</td>
      <td>53400000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Hereafter</td>
      <td>WB</td>
      <td>32700000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Edge of Darkness</td>
      <td>WB</td>
      <td>43300000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Hubble 3D</td>
      <td>WB</td>
      <td>52400000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Cop Out</td>
      <td>WB</td>
      <td>44900000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>14</th>
      <td>The Losers</td>
      <td>WB</td>
      <td>23600000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Splice</td>
      <td>WB</td>
      <td>17000000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Jonah Hex</td>
      <td>WB</td>
      <td>10500000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Flipped</td>
      <td>WB</td>
      <td>1800000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>18</th>
      <td>The Polar Express (IMAX re-issue 2010)</td>
      <td>WB</td>
      <td>673000.0</td>
      <td>2010</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Harry Potter and the Deathly Hallows Part 2</td>
      <td>WB</td>
      <td>381000000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>20</th>
      <td>The Hangover Part II</td>
      <td>WB</td>
      <td>254500000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Sherlock Holmes: A Game of Shadows</td>
      <td>WB</td>
      <td>186800000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Green Lantern</td>
      <td>WB</td>
      <td>116600000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Happy Feet Two</td>
      <td>WB</td>
      <td>64000000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Crazy, Stupid, Love.</td>
      <td>WB</td>
      <td>84400000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Contagion</td>
      <td>WB</td>
      <td>75700000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Unknown</td>
      <td>WB</td>
      <td>63700000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Dolphin Tale</td>
      <td>WB</td>
      <td>72300000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Sucker Punch</td>
      <td>WB</td>
      <td>36400000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Red Riding Hood</td>
      <td>WB</td>
      <td>37700000.0</td>
      <td>2011</td>
      <td>611</td>
      <td>Directed by Clint Eastwood, the mysterious dra...</td>
      <td>R</td>
      <td>Drama|Mystery and Suspense</td>
      <td>Clint Eastwood</td>
      <td>Brian Helgeland</td>
      <td>Oct 8, 2003</td>
      <td>Jun 8, 2004</td>
      <td>$</td>
      <td>88,800,000</td>
      <td>137 minutes</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Profitability

df_profit = merged_bom_tn_df['worldwide_gross'] - merged_bom_tn_df['production_budget']

print(df_profit)
```

    0       868879522.0
    1       675524642.0
    2       591244673.0
    3       638102828.0
    4       451156389.0
               ...     
    1242     -3910900.0
    1243     -3366889.0
    1244    -29351401.0
    1245     -4572344.0
    1246     -5544973.0
    Length: 1247, dtype: float64
    


```python
rt_df = pd.read_csv('zippedData/rt.movie_info.tsv.gz',sep='\t')

#creating a new data frame of genres and box_office.

director_box_df = rt_df[['director','box_office']]

print(director_box_df.head(20))
```

                   director  box_office
    0      William Friedkin         NaN
    1      David Cronenberg     600,000
    2        Allison Anders         NaN
    3        Barry Levinson         NaN
    4        Rodney Bennett         NaN
    5           Jay Russell         NaN
    6           Jake Kasdan  41,032,915
    7          Ray Lawrence     224,114
    8       Taylor Hackford     134,904
    9        Frank Marshall         NaN
    10                  NaN         NaN
    11                  NaN         NaN
    12                  NaN         NaN
    13     William Friedkin         NaN
    14        Peter Baldwin         NaN
    15  George Hickenlooper   1,039,869
    16                  NaN  99,165,609
    17       Rick Rosenthal         NaN
    18     Carl Erik Rinsch  20,518,224
    19         Jim Jarmusch   1,971,135
    


```python
#cleaning the new dataframe created

director_box_df.dropna(subset=['box_office'],inplace=True)
```

    C:\Users\User\AppData\Local\Temp\ipykernel_12160\3413251658.py:3: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      director_box_df.dropna(subset=['box_office'],inplace=True)
    


```python
director_box_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>director</th>
      <th>box_office</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>David Cronenberg</td>
      <td>600,000</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Jake Kasdan</td>
      <td>41,032,915</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Ray Lawrence</td>
      <td>224,114</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Taylor Hackford</td>
      <td>134,904</td>
    </tr>
    <tr>
      <th>15</th>
      <td>George Hickenlooper</td>
      <td>1,039,869</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1541</th>
      <td>Joss Whedon</td>
      <td>25,335,935</td>
    </tr>
    <tr>
      <th>1542</th>
      <td>Gauri Shinde</td>
      <td>1,416,189</td>
    </tr>
    <tr>
      <th>1545</th>
      <td>Sebastian Gutierrez</td>
      <td>59,371</td>
    </tr>
    <tr>
      <th>1546</th>
      <td>NaN</td>
      <td>794,306</td>
    </tr>
    <tr>
      <th>1555</th>
      <td>NaN</td>
      <td>33,886,034</td>
    </tr>
  </tbody>
</table>
<p>340 rows × 2 columns</p>
</div>




```python
import matplotlib.pyplot as plt

directors = ['David Cronenberg', 'Jake Kasdan', 'Ray Lawrence', 'Taylor Hackford', 'George Hickenlooper', 'Joss Whedon', 'Gauri Shinde', 'Sebastian Gutierrez']
box_office = [600000, 41032915, 224114, 134904, 1039869, 25335935, 1416189, 59371]

plt.barh(directors, box_office)
plt.xlabel('Box Office')
plt.ylabel('Director')
plt.title('Box Office by Director')
plt.show()
```


    
![png](output_87_0.png)
    


From the graph above, microsoft should consider Directors whose movies made more sales for instance, David Cronenberg and Joss Whedon in their studio.
