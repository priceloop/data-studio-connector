// general tutorial: https://medium.com/analytics-vidhya/creating-a-google-data-studio-connector-da7b35c6f8d5
// json example: https://raw.githubusercontent.com/googledatastudio/community-connectors/master/JSON-connect/src/main.js
// auth tutorial: https://developers.google.com/datastudio/connector/auth#oauth2_1
// oauth2-library: https://github.com/googleworkspace/apps-script-oauth2
// connector reference: https://developers.google.com/datastudio/connector/reference
// apps script reference: https://developers.google.com/apps-script/reference/data-studio

const cc = DataStudioApp.createCommunityConnector();

const nocodeHostName = "alpha.priceloop.ai"

const nocodeConfig = loadAppConfig();

function loadAppConfig() {
  const url = `https://${nocodeHostName}/app_config.json`;
  const nocodeConfig = fetchJSON(url, false)
  return nocodeConfig;
}

function throwDebugError(message) {
  cc.newDebugError()
    .setText(message)
    .throwException();
}

function throwUserError(message) {
  cc.newUserError()
    .setText(message)
    .throwException();
}

function isAdminUser() {
    return true;
}

// https://developers.google.com/datastudio/connector/reference#authtype
function getAuthType() {
  const AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.OAUTH2)
    .build();
}

function resetAuth() {
  getOAuthService().reset();
}

function isAuthValid() {
  return getOAuthService().hasAccess();
}

function authCallback(request) {
  var authorized = getOAuthService().handleCallback(request);
  if (authorized) {
    return HtmlService.createHtmlOutput('Success! You can close this tab.');
  } else {
    return HtmlService.createHtmlOutput('Access Denied. You can close this tab');
  };
};

function get3PAuthorizationUrls() {
  return getOAuthService().getAuthorizationUrl();
}

function getConfig() {
  const config = cc.getConfig();

  const url = `https://api.${nocodeHostName}/api/v1.0/workspaces`;
  const workspaceNames = fetchJSON(url);
  const workspaceTableNames = workspaceNames.map(workspaceName => {
    const url = `https://api.${nocodeHostName}/api/v1.0/workspaces/${workspaceName}`;
    const workspace = fetchJSON(url);

    return workspace.tables.map(table => `${workspace.name}/${table.name}`);
  }).flat();

  config
    .newInfo()
    .setId('instructions')
    .setText('Fill out the form to connect to your Priceloop Data.');

  const selectWorkspaceTable = config
      .newSelectSingle()
      .setId('workspaceTable')
      .setName('Enter the name of the Table you want to connect to')
      .setHelpText('e.g. workspace/table')

  workspaceTableNames.forEach(workspaceTableName => {
    selectWorkspaceTable.addOption(config.newOptionBuilder().setLabel(workspaceTableName).setValue(workspaceTableName))
  });

  // config
  //   .newCheckbox()
  //   .setId('cache')
  //   .setName('Cache response')
  //   .setHelpText('Useful with big datasets. Response is cached for 10 minutes.')
  //   .setAllowOverride(true);

  config.setDateRangeRequired(false);

  return config.build();
}

function getFields(request) {
  const fields = cc.getFields();
  const types = cc.FieldType;
  // const aggregations = cc.AggregationType;

  const [workspaceName, tableName] = request.configParams.workspaceTable.split('/');
  const url = `https://api.${nocodeHostName}/api/v1.0/workspaces/${workspaceName}/tables/${tableName}`;
  const table = fetchJSON(url); //fetchData(url, request.configParams.cache);

  function setupColumnType(typeName, columnId, columnName) {
    switch(typeName) {
      case "number":
        fields.newMetric()
          .setId(columnId)
          .setName(columnName)
          .setType(types.NUMBER);
        break;

      case "string":
        fields.newDimension()
          .setId(columnId)
          .setName(columnName)
          .setType(types.TEXT);
        break;

      case "boolean":
        fields.newDimension()
          .setId(columnId)
          .setName(columnName)
          .setType(types.BOOLEAN);
        break;

      case "date":
        fields.newDimension()
          .setId(columnId)
          .setName(columnName)
          .setType(types.YEAR_MONTH_DAY_SECOND); // precision to seconds expected: <year><month><day><hours><minutes><seconds>
        break;

      case "null":
        fields.newDimension()
          .setId(columnId)
          .setName(columnName)
          .setType(types.TEXT);
        break;

      default:
        throwUserError(`Unexpected type for column '${columnName}': ${typeName}`);
    }
  }

  table.columns.forEach((column, idx) => {
    const columnId = "" + (idx + 1) // idx + 1 because, the first column in the data result from our api is the priceloop_id (index 0). and id has to be a string.
    setupColumnType(column.tpe, columnId, column.name);
  });

  return fields;
}

function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

function getData(request) {
  const types = cc.FieldType;
  const fields = getFields(request);

  const requestedFieldIds = request.fields.map(field => field.name);
  const requestedFields = fields.forIds(requestedFieldIds);
  const requestedFieldsArray = requestedFields.asArray();

  const [workspaceName, tableName] = request.configParams.workspaceTable.split('/');

  const pageSize = 500;
  const urlByOffset = offset => `https://api.${nocodeHostName}/api/v1.0/workspaces/${workspaceName}/tables/${tableName}/data?offset=${offset*pageSize}&limit=${pageSize}`;

  var allDataRows = [];
  var currentOffset = 0;

  while (true) {
    const tableData = fetchJSON(urlByOffset(currentOffset)); //fetchData(url, request.configParams.cache);

    if (tableData.rows.length == 0) {
      break;
    }

    const dataRows = tableData.rows.map(row => {
      const columns = requestedFieldsArray.map(field => {
        const value = row[field.getId()];
        if (field.getType() == types.YEAR_MONTH_DAY_SECOND) {
          return convertDate(value);
        } else {
          return value;
        }
      });
      return { values: columns };
    });

    allDataRows.push(...dataRows)

    currentOffset++;
  }

  return {
    schema: requestedFields.build(),
    rows: allDataRows
  };
}

function getOAuthService() {
  return OAuth2.createService('Priceloop')
      .setPropertyStore(PropertiesService.getUserProperties())
      .setCache(CacheService.getUserCache())
      .setLock(LockService.getUserLock())
      .setAuthorizationBaseUrl(`https://auth.${nocodeHostName}/login`)
      .setTokenUrl(`https://auth.${nocodeHostName}/oauth2/token`)
      .setClientId(nocodeConfig.auth.clientId)
      .setClientSecret('_') // OAuth2 library says that clientSecret is required, but we do not have one in our app client in aws cognito. Empty string does not work, so put in anything.
      .setCallbackFunction('authCallback')
      .setScope(`${nocodeConfig.auth.apiScopes}`)
};

function fetchJSON(url, authorized = true) {
  try {
    const response = UrlFetchApp.fetch(url, {
      headers: authorized ? {
        Authorization: 'Bearer ' + getOAuthService().getAccessToken()
      } : undefined
    });
    const content = JSON.parse(response);
    return content;
  } catch (e) {
    throwUserError('Error querying "' + url + '": ' + e);
  }
}

function convertDate(dateString) {
  if (dateString == null) return null;

  const date = new Date(dateString);
  return (
    date.getUTCFullYear() +
    ('0' + (date.getUTCMonth() + 1)).slice(-2) +
    ('0' + date.getUTCDate()).slice(-2) +
    ('0' + date.getUTCHours()).slice(-2) +
    ('0' + date.getUTCMinutes()).slice(-2) +
    ('0' + date.getUTCSeconds()).slice(-2)
  );
}

// function getCachedData(url) {
//   var cacheExpTime = 600;
//   var cache = CacheService.getUserCache();
//   var cacheKey = url.replace(/[^a-zA-Z0-9]+/g, '');
//   var cacheKeyString = cache.get(cacheKey + '.keys');
//   var cacheKeys = cacheKeyString !== null ? cacheKeyString.split(',') : [];
//   var cacheData = {};
//   var content = [];

//   if (cacheKeyString !== null && cacheKeys.length > 0) {
//     cacheData = cache.getAll(cacheKeys);

//     for (var key in cacheKeys) {
//       if (cacheData[cacheKeys[key]] != undefined) {
//         content.push(JSON.parse(cacheData[cacheKeys[key]]));
//       }
//     }
//   } else {
//     content = fetchJSON(url);

//     for (var key in content) {
//       cacheData[cacheKey + '.' + key] = JSON.stringify(content[key]);
//     }

//     cache.putAll(cacheData);
//     cache.put(cacheKey + '.keys', Object.keys(cacheData), cacheExpTime);
//   }

//   return content;
// }

// function fetchData(url, cache) {
//   try {
//     var content = cache ? getCachedData(url) : fetchJSON(url);
//   } catch (e) {
//     throwUserError('Your request could not be fetched. The rows of your dataset might exceed the 100KB cache limit.');
//   }
//   if (!content) throwUserError('"' + url + '" returned no content.');

//   return content;
// }
