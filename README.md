# Priceloop Data Connector

## Authorize and enable

Install google clasp, login to Google and authorize clasp with:

```
clasp login
```

Then, enable Google Apps Script API in your [user settings](https://script.google.com/home/usersettings).

## Push connector code

```
clasp push
```

## Deploy new version of current connector code

```
clasp deploy
```

## Get Link for newest deployment

```
echo "https://datastudio.google.com/datasources/create?connectorId=$(clasp deployments | tail -n 1 | cut -d ' ' -f 2)&authuser=0"

```
