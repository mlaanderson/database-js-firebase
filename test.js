var driver = require('.');
var Connection = require('database-js').Connection;

(async function() {
    try {
        let conn = new Connection('firebase://user@example.com:password@statesdemo/ewJviY6wboTKJ57A2dZkvq8kxYo1?apiKey=AIzaSyD1ypTmnJb_d8ZOyfc-KBMe0tw8owYCwjA', driver);
        let stmt = conn.prepareStatement("SELECT SUM(State) FROM states");
        let rows = await stmt.query();
        
        console.log(rows);
        
        await conn.close();
        process.exit(0);
    } catch (err) {
        console.log(err);
        process.exit(1);
    }
})();