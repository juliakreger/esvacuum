esvacuum
========

# Description
This is a simple ruby gem which facilitates the bulk extraction of one elasticsearch cluster to another.

This gem deploys a script called esvacuum which can be executed, and the gem can be leveraged directly if so desired.

# Installation

Via GIT:

    git clone https://github.com/juliakreger/esvacuum.git
    cd esvacuum
    bundle install
    rake install

Via a Gemfile, use:

    gem 'esvacuum', git: 'git://github.com/juliakreger/esvacuum.git'

# Usage

## Usage via the gem

At present, the gem supports four arguments which must be passed in as a hash.  A little further down you can see an execution of the gem using IRB.

### Arguments & Description
* source      **REQUIRED** This is the URL for the source server.
* destination **REQUIRED** This is the URL for the destination server.
* size        *Default: 100* This is the chunk size for the operation.
* verbose     *Default: false* Output progress.

### Via irb

    $ irb
    1.9.3p448 :001 > require 'esvacuum'
     => true 
    1.9.3p448 :002 > Esvacuum.execute source: 'localhost:9200', destination: 'localhost:9201', size: 100, verbose: false
     => nil 
    1.9.3p448 :003 > exit
    $

## Usage via the esvacuum script

    $ esvacuum
    Usage: esvacuum ARGUMENTS

    Arguments
        -s, --source srcURL:9200         Required Source URL
        -d, --destination destURL:9200   Required Destination URL
        -c, --chunksize 100              Optional chunk size
        -h, --help                       help
    $

Note: Use of the script means that verbose output is always enabled.  If the gem is leveraged directly verbose output is automatically suppressed.


### An example exection.

    $ esvacuum -s localhost:9200 -d localhost:9201
    Processing Index: userprofiles
    Index userprofiles contains 12513 items.
    Records 500 completed in 0.93 seconds
    Records 1000 completed in 1.98 seconds
    .
    .
    .
    Records 12513 completed in 26.95 seconds
    Completed Index userprofiles in 26.955972 - 464.201 records/second
    Processing Index: users
    Index users contains 0 items.
    Completed Index users in 0.003387 - 0.0 records/second
    Completed
    $
