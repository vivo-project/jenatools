# jenatools
Tools for importing and exporting VIVO and Vitro triple stores (content and configuration) to and from various RDF formats.

These tools can be used to upgrade from Jena2 to Jena3 triple stores.  Export to TriG format using
jenatools and import the TriG format using jena3tools.

These tools can be used to export either Jena2 or Jena3 triple stores to other popular RDF formats
to create single files for sharing, and analytics.

*WARNING: This is beta software.  Please be sure your data is backed up and can be restored before using this software.
Please report any experiences with this software
to [vivo-tech@googlegroups.com](mailto:vivo-tech@googlegroups.com).  Thanks!*

## jena2tools

Import and export VIVO/Vitro SDB and TDB triples stores in Jena2 format from and to TriG and 
other RDF formats.

## jena3tools

Import and export TriG and other RDF formats to and from VIVO/Vitro SDB and TDB triple stores in 
Jena3 format.

## Distribution

jena2tools and jena3tools are distributed with VIVO/Vitro 2.0.0-beta releases and can be 
found in \<home\>/bin

## Usage

### jena2tools

    java -jar jena2tools [arguments] -d home_directory
    
    Arguments
    -d, --dir      REQUIRED. Specify the VIVO/Vitro home directory
    -i, --import   Import TriG format data to triple stores
    -e, --export   Export data from triple stores. Default format is TriG
    -h, --help     Display help text
    -f, --force    Force overwrite of previous exports
    -o, --output   Output format followed by one of nt, trig, rdf, or ttl
    
## jena3tools

    java -jar jena3tools [arguments] -d home_directory
        
    Arguments
    -d, --dir      REQUIRED. Specify the VIVO/Vitro home directory
    -i, --import   Import TriG format data to triple stores
    -e, --export   Export data from triple stores. Default format is TriG
    -h, --help     Display help text
    -f, --force    Force overwrite of previous exports
    -o, --output   Output format followed by one of nt, jsonld, trig, rdf, or ttl


