var db = require('.');

(async function () {
    let connection = db.open({
        Parameters: "apiKey=AIzaSyD1ypTmnJb_d8ZOyfc-KBMe0tw8owYCwjA",
        Hostname: 'statesdemo',
        Database: 'ewJviY6wboTKJ57A2dZkvq8kxYo1',
        Username: 'user@example.com',
        Password: 'password'
    });
    let stmt, results;
    results = await connection.query('SELECT * FROM states ORDER BY Ranking ASC LIMIT 10');
    console.log(results);
    await connection.close();
})();