Hadoop - Pokémon Data Analysis


Motivation and background

This project consists of analyzing different Pokémons’ data for different purposes. We focused on doing top ten lists and averages of certain values.
We chose this project because it seemed fun and creative. We grew up watching Pokémon on TV when we were kids and it was easy for us to make this project, because we are familiar with this subject.
The data set is represented by a csv file containing informations regarding Pokémons.  The information is organised in 10 columns, as follows: Number, Name, Type 1, Type 2, Total, Hit Points, Attack, Defense, Special Attack, Special Defense and Speed, each of them containing different values represented by strings or numbers.
For the processing part, the data set has to fulfill some well established structures. As the csv file is structured in 10 columns, there were a lot of empty cells on the Type 2 column which had to be filled with predefined notations in order to achieve the deserved form.
The purpose of this project consists in 5 tasks:

Find out the top 10 Pokémons based on their total power; 
Find out the top 10 fastest Pokémons;  
Find out the average HP (Hit Points) of all the Pokémon; 
Find out the top 10 Pokémon’s based on their special attack; 
Find out the average speed of all the Pokémon.		




Computational experiments

Before we began this project, we looked upon the WordCount code to see how a basic Hadoop project works. After that, we modified the code based on our needs.
Firstly, in the Mapper class we have to extract the informations we need. In order to do this, we are splitting the input data using a comma as a delimiter and then we have to select which column will be used in the processing part. According to the Text input format, we receive one line for each iteration of the Mapper. This line is split and then we collect the columns which are going to be used later in the Reducer class.
For the “Top 10 Pokémons” tasks, firstly, we created a custom class named TopPokemonWritable, a Writable object that stores two values, to help us define an useful data type and to override the compareTo method. Reducer part involves using a TreeSet having the size equal to 10. We overridden the reducer method so we can add in the TreeSet up to ten values who meet the required properties. If we have more that 10 records, we remove the one with the least value for the current property as the TreeSet is sorted in descending order. This order was achieved by the override we made to the compareTo method in the TopPokemonWritable class. Without this override, the TreeSet would have been sorted in ascending order by default. This way we have obtained the top 10 records from the data set.
The “Average” tasks were a little more trickier. To calculate an average, we need two values for each group: the sum of the values that we want to average and the number of values that went into the sum. These two values can be calculated in the Reducer class. The Mapper will output two columns of data, which will be processed in reduce method. For every value in valueList (i.e. see line 42 in the AverageSpeed class), we perform an addition to a predefined sum variable. For every map that comes in reducer, the cleanup method will calculate the average, so at the end we will have the main average that we need.
