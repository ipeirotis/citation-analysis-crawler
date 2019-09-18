This is the code for the **crawlers** that crawl **Google Scholar** for information 
about **authors** and **publications**.

## PREPARE THE ENVIRONMENT

    sudo apt-get install build-essential python-dev mysql-client libmysqlclient-dev python-dev libxml2-dev libxslt-dev python-lxml

**NOTE:** Crawlers can only run in a **Google Compute Engine** instance.

## CLONE THE CODE

    cd citation-analysis-crawler
    git submodule init
    git submodule update

## INSTALL THE DEPENDENCIES

    pip install -r requirements.txt

## CONFIGURE THE CRAWLER

Create `config.sh` based on `config.sh.sample`.

    cp config.sh.sample config.sh
    
Edit `config.sh`.

## RUN THE CRAWLER

    bash run.sh

**NOTE:** The supervisor that the crawler reports to must be up and running.
