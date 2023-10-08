# film_analysis-apachespark

# Movie Data Analysis

This Java program uses Apache Spark to perform analysis on movie data contained in a CSV file. The program performs several operations to obtain relevant information from the data, including calculating the total number of films, finding the film with the highest rating, counting films by genre, and calculating the average rating by genre.

## Use of the Program

The program is divided into different sections, each of which carries out a specific data analysis:

### Calculation of the Total Number of Films

In the first part of the code, the program calculates the total number of movies present in the CSV file.

### Find the Film with the Highest Rating

In this section, the program identifies the film with the highest rating among those present in the file. The film title and rating are printed on the console.

### Film Count by Genre

The third part of the program creates a table of movie data and calculates the number of movies for each genre. The results are displayed in tabular mode using the `show()` method.

### Calculation of the Average Rating by Genre

In the last section of the code, the genres are sorted alphabetically and the average rating for each genre is calculated. These results are also shown in tabular mode.

## Requirements

Before running the program, make sure you have the following:

- [Apache Spark](https://spark.apache.org/) installed and configured in your environment.
- A CSV file containing movie data. Make sure you set the file path correctly in your code.

## Program Execution

To run the program, follow these steps:

1. Make sure Apache Spark is configured correctly in your environment.

2. Change the path to the CSV file in your code to match your system.
