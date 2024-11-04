# libqhash

A library for easy C hashtables

Check the header file or language server for more information.

## struct idm\_list
Is a singly-linked list of unsigned numbers used for id management. It provides basic push and pop functionality.

## struct idm
Is a structure for id management. Basically a "last" id, and an idm\_list of free ids.

## hash
General hashtable functions are prefixed with "hash\_" and they alone allow for most of the things you will need.

Many others are just provided for easy use.

## uhash
Is a hashtable in which keys are unsigned ints.

## lhash
Is a uhash, but they also have automatic id management capabilities.

## ahash
Is a uhash in which values are also unsigned. Its purpose is to have easy n to n associations.

## shash
A hashtable in which keys are strings.

## suhash
You guessed it. Keys are strings, values are unsigned.

## sphash
String to pointer.

## ushash
Unsigned to string.

# qhash

A binary for easy and fast hashtables / indexes. It provides most common use-cases.

## Usage

For help:
```sh
qhash -?
```

This will you a listing of ids and pet owner names:
```sh
qhash -l owners.db
```

This will put a pet owner into the database, and output his id:
```sh
qhash -p Mathew owners.db # example output: 4
```

Then insert pets into the pet database:
```sh
qhash -p cat -p dog pets.db # example output: 2 and 3
```

And finally create a database that says that Mathew owns both:
```sh
qhash -m1 -p 4:2 -p 4:3 assoc.db
```

Or list all of Mathew's pets (names included):
```sh
qhash -a pets.db -g4 assoc.db
```

And now get a random pet name that corresponds to Mathew:
```sh
qhash -q owners.db -a pets.db -RMathew assoc.db # example output: dog
```
