**Introduction**

The PhonePe Pulse Dashboard is a user-friendly Streamlit application designed to provide insights into user engagement, transaction trends, and insurance-related data across states, districts, and pincodes. The data is sourced from the official PhonePe Pulse GitHub repository, converted from JSON to CSV format, and stored in a MySQL database to enable efficient querying and analysis.

**Project Overview**

Phonepe Pulse Data Dashboard consists of following elements.

  * Data Cloning from Official Phonepe Pulse website
  * Data ETL onto the MYSQL server
  * Streamlit Interactive and intuitive dashboard
  * Added features to easily filter and search data
  * Created interactive charts to show the data clearly
  * Shared insights from pulse data

**Table Of Contents**

  1. Key technologies
  2. Installation
  3. Usage
  4. Features
  5. References

**1. Key Technologies**

  * GIT
  * Python
  * Pandas
  * Numpy
  * ETL
  * Data Visualization (Plotly/Seaborn/Matplotlib)
  * JSON
  * SQL (MYSQL, SQLAlchemy)
  * Geopandas
  * Streamlit

**2. Installation**

To run this application, you need to follow these below steps

  1. Install Python on your machine
  2. Install required libraies using pip
     
        **pip install pandas pymysql streamlit plotly sqlalchemy gitpython numpy mysql-connector-python**

  3. Install and configure MYSQL server on your machine

**3. Usage**

Once the project application is running, users can access the application in web browser. Select page to check the analysis and visualization Inference for user, transaction and insurance data.

  1. Merge the ETL data to MYSQL Server: PHONEPE PULSE DB ETL.ipynb
  2. Configure application with your database connection details
  3. Run the application via CLI: phonepe_web_app.py

**4. Features**

Phonepe Pulse Data Insights Dashboard provides following features:

  1. Finance Analysis
  2. Dashboard Development
  3. Interactive Analysis
  4. Geospatial Visualization

**5. References**

Python Documentation: https://docs.python.org/

Pandas Documentation: https://pandas.pydata.org/docs/

Plotly Documentation: https://plotly.com/python/

MYSQL Documentation: https://dev.mysql.com/doc/

SQLAlchemy Documentation:https://docs.sqlalchemy.org/

Streamlit Documentation: https://docs.streamlit.io/

Git Documentation: https://git-scm.com/doc

Numpy Documentation: https://numpy.org/doc/stable/user/

