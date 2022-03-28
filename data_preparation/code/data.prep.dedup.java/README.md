# Data Preparation for Duplicate Detection

This project implements the preparation process described in the paper "Data Preparation for Duplicate Detection".

## Installation

Before executing the algorithms, it is necessary to install Java JDK 1.8 or later. A mvn clean package should generate the executable Jar file.

Manual installation of jar files into your local Maven repository:
mvn install:install-file -Dfile=src/main/resources/JHyphenator-1.0.jar -DgroupId=JHyphenator -DartifactId=jhyphenator -Dversion=1.0 -Dpackaging=jar

## Execution

It is advised to execute the different modules through [data.prep.dedup.python](https://gitlab.com/data.prep.dedup/data.prep.dedup.python), which is developed to automate the execution across steps and ease the arguments selection.

A brief description of the main packages:
*  similarities: Pair similarities are calculated here.
*  preparation: This is where the main data preparation takes place. Preparator implementations are located at the class: deduplication.preparation.Preparators.
*  nonduplicates: Non-duplicates are generated here.
  
## Storage

We use MySQL to store our results and the configuration is stored in the python project. An overview of the storage model, as it is created during execution:
*  Dataset records (original): The original records are stored here and every relation has a different number of attributes, according to the dataset.
*  Dataset pairs (original): The pairs of every dataset, along with their similarity, which is used for the silverstandard selection.
*  Data cube: Apart from the basic information of record ID, dataset, attribute, and value, we also store the preparators that are applied. Every preparator corresponds to a different Boolean attribute.
*  Similarities cube: Similar to the "Data cube" every preparator is a Boolean attribute and we also keep the record pair IDs 1 and 2, dataset, and attribute.
*  Similarities difference cube: Finally, based on the "Similarities cube", this relational table also contains the preparators applied, but also the similarities before and after the preparation, as well as their difference.