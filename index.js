var firebase = require('firebase');
var parse = require('node-sqlparser').parse;

var m_root = Symbol('root');
var m_credentials = Symbol('credentials');
var m_authenticated = Symbol('authenticated');
var m_authenticator = Symbol('authenticator');


class Firebase {
    /**
     * Creates an instance of Firebase.
     * @param {string} root 
     * @param {object} credentials 
     * @memberof Firebase
     */
    constructor(root, credentials) {
        var self = this;
        var resolveAuth;

        this[m_root] = firebase.database().ref(root);
        this[m_credentials] = credentials;
        this[m_authenticated] = false;
        this[m_authenticator] = new Promise((resolve, reject) => {
            resolveAuth = resolve;
        });

        function onAuth(auth) {
            if (auth) {
                this[m_authenticated] = true;
                resolveAuth();
            } else {
                this[m_authenticated] = false;
                firebase.auth().signInWithEmailAndPassword(credentials.email, credentials.password);
            }
        }

        firebase.auth().onAuthStateChanged(onAuth);
    }

    /**
     * Tests the passed row based on the where array.
     * This could be faster on the server if the data has been
     * indexed, and if the user only wants a single WHERE.
     * 
     * @param {object} where Where object from the SQL Parser
     * @param {object} row Row to compare
     * @returns {boolean} if the row matches the where object
     * @memberof Firebase
     */
    doWhere(where, row, namespace) {
        if (where === null) return true;
        namespace = !!namespace;

        var getVal = (obj) => {
            let field = namespace ? obj.table + "." + obj.column : obj.column;
            if (obj.type === "column_ref") return row[field];
            if (obj.type === "binary_expr") return this.doWhere(obj, row);
            return obj.value;
        }

        var replaceIfNotPrecededBy = (notPrecededBy, replacement) => {
            return function(match) {
                return match.slice(0, notPrecededBy.length) === notPrecededBy
                ? match
                : replacement;
            }
        }

        var like2RegExp = (like) => {
            var restring = like;
            restring = restring.replace(/([\.\*\?\$\^])/g, "\\$1");
            restring = restring.replace(/(?:\\)?%/g, replaceIfNotPrecededBy('\\', '.*?'));
            restring = restring.replace(/(?:\\)?_/g, replaceIfNotPrecededBy('\\', '.'));
            restring = restring.replace('\\%', '%');
            restring = restring.replace('\\_', '_');
            return new RegExp('^' + restring + '$');
        }

        switch (where.type) {
            case "binary_expr":
                switch(where.operator) {
                    case "=":
                        return getVal(where.left) == getVal(where.right);
                    case "!=":
                    case "<>":
                        return getVal(where.left) != getVal(where.right);
                    case "<":
                        return getVal(where.left) < getVal(where.right);
                    case "<=":
                        return getVal(where.left) <= getVal(where.right);
                    case ">":
                        return getVal(where.left) > getVal(where.right);
                    case ">=":
                        return getVal(where.left) >= getVal(where.right);
                    case "AND":
                        return getVal(where.left) && getVal(where.right);
                    case "OR":
                        return getVal(where.left) && getVal(where.right);
                    case "IS":
                        return getVal(where.left) === getVal(where.right)
                    case "LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === true;
                    case "NOT LIKE":
                        return like2RegExp(getVal(where.right)).test(getVal(where.left)) === false;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    /**
     * Used to push a row into the data object. If the fields are limited
     * in the query, only places the requested fields.
     * 
     * @param {object} sqlObj 
     * @param {Array} data 
     * @param {object} row 
     * @returns 
     * @memberof Firebase
     */
    chooseFields(sqlObj, data, row, namespace) {
        if (sqlObj.columns === "*") {
            data.push(row);
            return;
        }

        namespace = !!namespace;

        let isAggregate = sqlObj.columns.some((col) => { return col.expr.type === 'aggr_func'; });

        if (isAggregate === true) {

            var groupby = () => {
                if (sqlObj.groupby == null) {
                    if (data.length < 1) {
                        data.push({});
                    }
                    return 0;
                }
                let result = data.findIndex(drow => {
                    return sqlObj.groupby.every(group => drow[group.column] == row[group.column]);
                });

                if (result <= 0) {
                    data.push({});
                    return data.length - 1;
                }
                
                return result;
            }

            var index = groupby();

            for (let col of sqlObj.columns) {
                let name, data_row;
                switch(col.expr.type) {
                    case 'column_ref':
                        name = col.as || col.expr.column;
                        data[index][name] = row[col.expr.column];
                        break;
                    case 'aggr_func':
                        name = col.as || col.expr.name.toUpperCase() + "(" + col.expr.args.expr.column + ")";
                        
                        switch(col.expr.name.toUpperCase()) {
                            case 'SUM':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name] += row[col.expr.args.expr.column];
                                break;
                            case 'COUNT':
                                if (data[index][name] === undefined) {
                                    data[index][name] = 0;
                                }
                                data[index][name]++;
                                break;
                        }
                        break;
                }
            }
        } else {
            let result = {};
            for (let col of sqlObj.columns) {
                let field = namespace ? col.expr.table + "." + col.expr.column : col.expr.column;
                let name = col.as || field;
                result[name] = row[field];
            }
            data.push(result);
        }
    }

    doSingleSelect(sqlobj, rows, namespace) { 
        let result = [];
        namespace = !!namespace;

        // apply where and group by
        rows = rows.filter(row => this.doWhere(sqlobj.where, row, namespace));

        // apply order by
        if (sqlobj.orderby) {
            rows.sort((a, b) => {
                for (let orderer of sqlobj.orderby) {
                    let column = namespace ? orderer.expr.table + "." + orderer.expr.column : orderer.expr.column;
                    if (orderer.expr.type !== 'column_ref') {
                        throw new Error("ORDER BY only supported for columns, aggregates are not supported");
                    }

                    if (a[orderer.expr.column] > b[column]) {
                        return orderer.type == 'ASC' ? 1 : -1;
                    }
                    if (a[orderer.expr.column] < b[column]) {
                        return orderer.type == 'ASC' ? -1 : 1;
                    }
                }
                return 0;
            });
        }

        // pick only the fields the query specifies
        rows.map(row => this.chooseFields(sqlobj, result, row, namespace));

        // apply limits
        if (sqlobj.limit) {
            if (sqlobj.limit.length !== 2) {
                throw new Error("Invalid LIMIT expression: Use LIMIT [offset,] number");
            }
            let offs = parseInt(sqlobj.limit[0].value);
            let len = parseInt(sqlobj.limit[1].value);
            result = result.slice(offs, offs + len);
        }

        return result;
    }
    
    join(dest, src, query, includeAllDest, includeAllSrc, namespace = false) {
        var rows = [];

        let destRows = dest.map(row => {
            return { used: false, row: row };
        });

        let srcRows = src.map(row => {
            return { used: false, row: row };
        });

        for (let destRow of destRows) {

            for (let srcRow of srcRows) {
                var bigrow = {}
                for (var k in destRow.row) { bigrow[k] = destRow.row[k]; }
                for (var k in srcRow.row) { bigrow[k] = srcRow.row[k]; }
                if (this.doWhere(query, bigrow, namespace)) {
                    rows.push(bigrow);
                    destRow.used = true;
                    srcRow.used = true;
                }
            }
        }

        if (includeAllDest) {
            destRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }
        if (includeAllSrc) {
            srcRows.filter(row => row.used == false).map(row => rows.push(row.row));
        }

        return rows;
    }


    /**
     * Performs an SQL SELECT. This is called from a Promise.
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlObj 
     * @returns 
     * @memberof Firebase
     */
    doSelect(resolve, reject, sqlObj) {
        var promises = [];
        var namespace = sqlObj.from.length > 1;

        for (var n = 0; n < sqlObj.from.length; n++) {
            promises.push(this[m_root].child(sqlObj.from[n].table).once('value'));
        }

        Promise.all(promises).then((snapshots) => {
            // populate the table objects
            var tables = snapshots.map((snapshot, n) => {
                var result = {
                    from: sqlObj.from[n],
                    name: sqlObj.from[n].table,
                    rows: []
                };

                var rows = Object.values(snapshot.val());
                if (namespace) {
                    result.rows = rows.map(row => {
                        var nsRow = {}
                        for (var key in row) {
                            nsRow[sqlObj.from[n].table + "." + key] = row[key];
                        }
                        return nsRow;
                    });
                } else {
                    result.rows = rows;
                }

                return result;
            });

            // perform joins
            while (tables.length > 1) {
                switch(tables[1].from.join) {
                    case 'INNER JOIN':
                        tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, false, namespace);
                        break;
                    case 'LEFT JOIN':
                        tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, false, namespace);
                        break;
                    case 'RIGHT JOIN':
                        tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, false, true, namespace);
                        break;
                    case 'FULL JOIN':
                        tables[0].rows = this.join(tables[0].rows, tables[1].rows, tables[1].from.on, true, true, namespace);
                        break;
                }
                tables.splice(1,1);
            }

            // the join is performed, do a single select on the resulting big table
            let result = this.doSingleSelect(sqlObj, tables[0].rows, namespace);

            resolve(result);
        });
    }

    /**
     * Performs an SQL UPDATE. This is called from a Promise
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlObj 
     * @memberof Firebase
     */
    doUpdate(resolve, reject, sqlObj) {

        this[m_root].child(sqlObj.table).once('value').then((snapshot) => {
            let raw = snapshot.val();
            let rows = [];
            let promises = [];
            let updateObj = {};

            for (let item of sqlObj.set) {
                updateObj[item.column] = item.value.value;
            }

            for (let row_id in raw) {
                if (this.doWhere(sqlObj.where, raw[row_id]) === true) {
                    promises.push(this[m_root].child(`${sqlObj.table}/${row_id}`).update(updateObj));
                    rows.push(row_id);
                }
            }
            Promise.all(promises).then((values) => {
                resolve(rows);
            }).catch((reason) => {
                reject(reason);
            });
        });
    }

    /**
     * Performs an SQL INSERT. This is called from a Promise.
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlObj 
     * @memberof Firebase
     */
    doInsert(resolve, reject, sqlObj) {
        let rows = [];
        for (let i = 0; i < sqlObj.values.length; i++) {
            let data = {};
            for (let n = 0; n < sqlObj.columns.length; n++) {
                data[sqlObj.columns[n]] = sqlObj.values[i].value[n].value;
            }
            

            rows.push(this[m_root].child(sqlObj.table).push(data));
        }
        Promise.all(rows).then((values) => {
            resolve(values.map((o) => o.key));
        }).catch((reason) => {
            reject(reason);
        });
    }

    /**
     * Performs an SQL DELETE. This is called from a Promise
     * 
     * @param {function} resolve 
     * @param {function} reject 
     * @param {any} sqlObj 
     * @memberof Firebase
     */
    doDelete(resolve, reject, sqlObj) {
        this[m_root].child(sqlObj.from[0].table).once('value').then((snapshot) => {
            let raw = snapshot.val();
            let promises = [], rowIds = [];
            for (let row_id in raw) {
                if (this.doWhere(sqlObj.where, raw[row_id]) === true) {
                    rowIds.push(row_id);
                    promises.push(this[m_root].child(sqlObj.from[0].table).child(row_id).remove());
                }
            }
            Promise.all(promises).then((values) => {
                resolve(rowIds);
            }).catch((reason) => {
                reject(reason);
            });
        });
    }

    /**
     * Runs the SQL statement
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    runSQL(sql) {
        var self = this;
        return new Promise((resolve, reject) => {
            this[m_authenticator].then(() => {
                // we are now authenticated
                let sqlObj;
                try {
                    sqlObj = parse(sql);
                } catch (err) {
                    reject(err);
                }

                switch(sqlObj.type) {
                    case 'select':
                        this.doSelect(resolve, reject, sqlObj);
                        break;
                    case 'update':
                        this.doUpdate(resolve, reject, sqlObj);
                        break;
                    case 'insert':
                        this.doInsert(resolve, reject, sqlObj);
                        break;
                    case 'delete':
                        this.doDelete(resolve, reject, sqlObj);
                        break;
                    default:
                        resolve(sqlObj);
                        break;
                }
            });
        });
    }

    /**
     * Executes the passed SQL
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    execute(sql) {
        return this.runSQL(sql);
    }

    /**
     * Executes the passed SQL
     * 
     * @param {string} sql 
     * @returns {Promise<array>} Promise of array of selected rows, updated rows, inserted rows, or deleted row Firebase keys
     * @memberof Firebase
     */
    query(sql) {
        return this.runSQL(sql);
    }

    /**
     * Closes the connection, sets Firebase to offline mode.
     * 
     * @returns {Promise<boolean>}
     * @memberof Firebase
     */
    close() {
        firebase.database().goOffline();
        return Promise.resolve(true);
    }
}

module.exports = {
    /**
     * Opens the connection using the connection object.
     * @param {object} connection
     * @returns {Firebase}
     */
    open: function(connection) {
        let params = {};
        let paramArray = connection.Parameters ? connection.Parameters.split(/[&]/g) : [];

        paramArray.map((p) => {
            let parts = p.split('=');
            params[parts[0]] = parts[1];
        });

        let config = {
            apiKey: params.apiKey || "",
            authDomain: connection.Hostname + ".firebaseapp.com",
            databaseURL: "https://" + connection.Hostname + ".firebaseio.com",
            projectId: connection.Hostname,
            storageBucket: connection.Hostname + ".appspot.com",
            messagingSenderId: params.messagingSenderId || ""
        }


        firebase.initializeApp(config);
        return new Firebase(connection.Database, {
            email: connection.Username,
            password: connection.Password
        });
    }
};