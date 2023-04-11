#!/bin/bash

mkdir ./class_diags/

# linkers=("duckdb", "spark")
linkers=("duckdb" "spark" "sqlite" "athena")

for linker in "${linkers[@]}"
do
    cp ./splink/linker.py ./splink/"$linker"/
    cp ./splink/__init__.py ./splink/"$linker"/
    cp ./splink/splink_dataframe.py ./splink/"$linker"/
    # create class diagrams, save to folder and rename
    pyreverse -o png ./splink/"$linker" -d ./class_diags/ -p "$linker"

    rm ./splink/"$linker"/linker.py ./splink/"$linker"/__init__.py ./splink/"$linker"/splink_dataframe.py 

done

