var db = require('.');

var connection = db.open({
    Parameters: "apiKey=AIzaSyD1ypTmnJb_d8ZOyfc-KBMe0tw8owYCwjA",
    Hostname: 'statesdemo',
    Database: 'ewJviY6wboTKJ57A2dZkvq8kxYo1',
    Username: 'user@example.com',
    Password: 'password'
});

function handleError(error) {
    console.log("ERROR:", error);
    process.exit(1);
}

connection.query("SELECT * FROM states WHERE State = 'South Dakota'").then((data) => {
    if (data.length != 1) {
        handleError(new Error("Invalid data returned"));
    }
    connection.close().then(() => {
        process.exit(0);
    }).catch(handleError);
}).catch(handleError);