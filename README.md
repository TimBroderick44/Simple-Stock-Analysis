# Apple Vs Microsoft

NOTE: Project is not yet complete (working on transitioning to use EC2 and S3 properly. The below was done jumping between the EC2 instance and locally)

This full-stack Python web application fetches minute-by-minute stock data on Microsoft and Apple from the Alpha Vantage API, processes the data using Spark, and then stores it in an AWS S3 bucket. The data is then read and visualized using Chart.js on a Flask web server hosted on an AWS EC2 instance. 

The visualizations available to the user include:
- Daily average closing prices
- Hourly maximum prices
- Daily trading volumes
- Moving average of closing prices

## Table of Contents
- [Apple Vs Microsoft](#apple-vs-microsoft)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Screenshots](#screenshots)
  - [Highlights](#highlights)
  - [Lessons Learned](#lessons-learned)
  - [Difficulties](#difficulties)
  - [Future Implementations](#future-implementations)

## Description



## Screenshots

![Daily Averages](./assets/daily.png)
![Hourly Max Prices](./assets/hourly.png)
![Daily Volume](./assets/volume.png)
![Moving Average Close](./assets/average.png)

## Highlights

- Utilizes the Alpha Vantage API to fetch detailed stock data.
- Processes data using Spark (currently only utilising two cores)
- Stores and retrieves data from an AWS S3 bucket.
- Visualizes data using Chart.js for interactive and informative charts.
- Flask backend is served on an AWS EC2 instance. 

## Lessons Learned

- Utilizing an EC2 instance and implementing security rules, roles and other settings through the AWS management console. 
- Integrating and utilizing APIs for real-time data retrieval.
- Utilizing Spark to process large datasets efficiently (e.g. using more than one CPU core).
- Storing data in an AWS S3 bucket and accessing it through the web application.
- Implementing data visualization techniques to present data effectively.
- Running a Flask backend off an EC2 instance 
- Building a Flask application and serving dynamic content.
- Better understanding of Python and its ecosystem.

## Difficulties

- Handling large datasets and ensuring efficient processing.
- There were often situations where the memory required by Spark was greater than the memory provided by the EC2 instance (1gb) so had to do that locally.
- Debugging issues related to data inconsistencies. Having so many steps involved with the processing of the data allowed for more errors.
- Ensuring smooth integration between different components of the application. I had a lot of trouble (at first) setting up the EC2 instance and making sure that the data being processed could be transferred to and from the S3 bucket. 
- Couldn't set up the SSH tunnel extention on VSCode so through the terminal accessed the EC2 instance and coded in Nano. ( In hindsight, great experience.) 

## Future Implementations

- Adding more stock symbols for comparison.
- Implementing more sophisticated data analysis techniques.
- Enhancing the user interface for better user experience.
- Implementing real-time data updates for the charts.
- Making greater use of AWS services 