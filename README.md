# dynamodb-single-table-example

Example project to demonstrate simple relational modelling with a single DynamoDB table using the Northwind dataset.
Inspired by https://github.com/trek10inc/ddb-single-table-example and this [blog post](https://trek10.com/blog/dynamodb-single-table-relational-modeling/).

## Using the code

### Requirements
* Golang
* Make


### Building the project

```
make build
```


### Creating the DynamoDB table

```
bin/ddb-single-table-cli create-table
```


### Loading the data

```
bin/ddb-single-table-cli load-table-data
```

This loads the files in the `csv` folder into the table according to the data access patterns defined in the blog post.


### Running the queries

```
bin/ddb-single-table-cli run-queries
```
