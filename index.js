var firebase = require('firebase');
var parse = require('node-sqlparser').parse;

var m_root = Symbol('root');
var m_credentials = Symbol('credentials');
var m_authenticated = Symbol('authenticated');
var m_authenticator = Symbol('authenticator');


class Firebase {
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

    doWhere(where, data) {
        if (where === null) return true;
        var self = this;

        function getVal(obj) {
            if (obj.type === "column_ref") return data[obj.column];
            if (obj.type === "binary_expr") return self.doWhere(obj, data);
            return obj.value;
        }

        switch (where.type) {
            case "binary_expr":
                switch(where.operator) {
                    case "=":
                        return getVal(where.left) == getVal(where.right);
                    case "!=":
                    case "<>":
                        return getVal(where.left) != getVal(where.right);
                    case "AND":
                        return getVal(where.left) && getVal(where.right);
                    case "OR":
                        return getVal(where.left) && getVal(where.right);
                    case "IS":
                        return getVal(where.left) === getVal(where.right)
                    default:
                        return false;
                }
                break;
            default:
                return false;
        }
    }

    chooseFields(sqlObj, data, row) {
        if (sqlObj.columns === "*") {
            data.push(row);
            return;
        }

        let isAggregate = sqlObj.columns.some((col) => { return col.expr.type === 'aggr_func'; });

        if (isAggregate === true) {
            if (data.length === 0) {
                data.push({});
            }

            for (let col of sqlObj.columns) {
                let name, data_row;
                switch(col.expr.type) {
                    case 'column_ref':
                        name = col.as || col.expr.column;
                        data[0][name] = row[col.expr.column];
                        break;
                    case 'aggr_func': // TODO implement group by
                        name = col.as || col.expr.name.toUpperCase() + "(" + col.expr.args.expr.column + ")";
                        
                        switch(col.expr.name.toUpperCase()) {
                            case 'SUM':
                                if (data[0][name] === undefined) {
                                    data[0][name] = 0;
                                }
                                data[0][name] += row[col.expr.args.expr.column];
                                break;
                            case 'COUNT':
                                if (data[0][name] === undefined) {
                                    data[0][name] = 0;
                                }
                                data[0][name]++;
                                break;
                        }
                        break;
                }
            }
        } else {
            let result = {};
            for (let col of sqlObj.columns) {
                let name = col.as || col.expr.column;
                result[name] = row[col.expr.column];
            }
            data.push(result);
        }
    }

    doSelect(resolve, reject, sqlObj) {
        if (sqlObj.from.length !== 1) {
            return reject("Selects from more than one table are not supported");
        }
        
        if (sqlObj.groupby !== null) {
            console.warn("GROUP BY is unsupported");
        }

        if (sqlObj.limit !== null) {
            console.warn("LIMIT is unsupported");
        }

        this[m_root].child(sqlObj.from[0].table).once('value').then((snapshot) => {
            let raw = snapshot.val();
            let rows = [];
            for (let row_id in raw) {
                if (this.doWhere(sqlObj.where, raw[row_id]) === true) {
                    this.chooseFields(sqlObj, rows, raw[row_id]);
                }
            }

            if (sqlObj.orderby) {
                rows.sort((a, b) => {
                    for (let orderer of sqlObj.orderby) {
                        if (orderer.expr.type !== 'column_ref') {
                            throw new Error("ORDER BY only supported for columns, aggregates are not supported");
                        }

                        if (a[orderer.expr.column] > b[orderer.expr.column]) {
                            return orderer.type = 'ASC' ? 1 : -1;
                        }
                        if (a[orderer.expr.column] < b[orderer.expr.column]) {
                            return orderer.type = 'ASC' ? -1 : 1;
                        }
                    }
                    return 0;
                });
            }

            resolve(rows);
        });
    }

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

    runSQL(sql) {
        var self = this;
        return new Promise((resolve, reject) => {
            this[m_authenticator].then(() => {
                // we are now authenticated
                let sqlObj;
                try {
                    sqlObj = parse(sql);
                } catch (err) {
                    // deletes aren't yet supported by the node-sqlparser
                    // so fake a SELECT and then change the type after the parse
                    if (/^delete/i.test(sql) === true) {
                        sql = sql.replace(/^delete/i, 'SELECT * ');
                        sqlObj = parse(sql);
                        sqlObj.type = 'delete';
                        delete sqlObj.columns;
                    } else {
                        reject(err);
                    }
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

    execute(sql) {
        return this.runSQL(sql);
    }

    query(sql) {
        return this.runSQL(sql);
    }

    close() {
        firebase.database().goOffline();
        return Promise.resolve(true);
    }
}

module.exports = {
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