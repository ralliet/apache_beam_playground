# Beam example

The purpose of this git repo is to learn how to work with Apache Beam and test a few basic things.
Everything runs locally. Maybe in the future connecting with google cloud platform.

## Installation

```
    pip install -r requirements.txt
```

## Execution

Start the instance with:

```
    make run
```

Hit the following endpoint:

`http://localhost:8080/dataflow/transform_titanic_data`

If the transformation of the dataset was OK it should give the following message:

`All survivors are writte to data/output/titanic.txt-00000-of-00001`
