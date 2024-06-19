# Flight Data Assessment

This Scala project utilizes Apache Spark for processing flight data. The project includes Spark Core and Spark SQL functionalities, managed with SBT (Simple Build Tool).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)

## Prerequisites

Before you can build and run this project, ensure you have the following software installed on your system:

1. **Java Development Kit (JDK) 1.8**
    - [Download JDK 1.8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)

2. **Scala 2.12.10**
    - [Download Scala 2.12.10](https://www.scala-lang.org/download/2.12.10.html)

3. **SBT (Simple Build Tool)**
    - [Download SBT](https://www.scala-sbt.org/download.html)

4. **Spark-core 2.4.8**
    - [Spark-core 2.4.8](https://mvnrepository.com/artifact/org.apache.spark/spark-core)

5. **Spark-sql 2.4.8**
    - [Spark-sql](https://mvnrepository.com/artifact/org.apache.spark/spark-sql)

- You can add directly the following code snippet for the Spark version 2.4.8 in built.sbt:\
**libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.4.8",\
"org.apache.spark" %% "spark-sql" % "2.4.8"
)**
## Installation

**Clone the Repository:**

   ```sh
   git clone https://github.com/Maitree-5/QuantexaAssessment
   cd QuantexaAssessment
