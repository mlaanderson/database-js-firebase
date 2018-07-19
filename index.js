const AbstractDriver = require("database-js-sqlparser");
var firebase = require('firebase');

var m_root = Symbol('root');
var m_credentials = Symbol('credentials');
var m_authenticated = Symbol('authenticated');
var m_authenticator = Symbol('authenticator');


class Firebase extends AbstractDriver {
    /**
     * Creates an instance of Firebase.
     * @param {string} root 
     * @param {object} credentials 
     * @memberof Firebase
     */
    constructor(root, credentials) {
        super();
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
     * Load all rows from a given table. Promise returns each row associated with
     * and index value that is string or integer
     * @param {string} table The table name to load rows from
     * @returns {Promise<{[key:string|number]:any}>} 
     */
    load(table) {
        return new Promise((resolve, reject) => {
            this[m_root].child(table).once('value').then((snapshot) => {
                resolve(snapshot.val());
            }).catch(err => reject(err));;
        });
    }

    /**
     * Stores a row into the table
     * @param {string} table The name of the destination table
     * @param {number|string} index The array index or object key for the table row, null to insert
     * @param {any} row The data to store
     * @returns {number|string} Then index or object key which was stored
     */
    store(table, index, row) {
        return new Promise((resolve, reject) => {
            if (index) {
                this[m_root].child(table).child(index).set(row).then(() => resolve(index)).catch(err => reject(err));
            } else {
                var ref = this[m_root].child(table).push(row).then(() => resolve(ref.key)).catch(err => reject(err));
            }
        });
    }

    /**
     * Removes a row from the table
     * @param {string} table The name of the table
     * @param {number|string} index The array index or object key for the table row
     */
    remove(table, index) {
        return new Promise((resolve, reject) => {
            this[m_root].child(table).child(index).remove().then(() => resolve(index)).catch(err => reject(err));
        });
    }

    /**
     * Creates a new table - actually does nothing since Firebase doesn't need the definition created
     * @param {string} table The name of the table to create
     * @param {Array<{name:string,index:number,type:string,length?:number,pad?:string}>} definition The definition of the table to create
     */
    create(table, definition) {
        return Promise.resolve(true);
    }

    /**
     * Drops a table, deletes all the data associated with the table
     * @param {string} table The name of the table to drop
     */
    drop(table) {
        return new Promise((resolve, reject) => {
            this[m_root].child(table).remove().then(() => resolve(true));
        });
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

    ready() {
        return this[m_authenticator];
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