var controller = require('../controllers/controller');
exports.routes = function(app) {

   // app.get('/time', controller.times);
    app.get('/ips', controller.getIPS);
    app.get('/docs', controller.getDocs);
    app.get('/years', controller.getYears);
    app.get('/times', controller.times);
    app.get('/prices', controller.prices);
    app.get('/recipes', controller.recipes);
    app.get('/medicines', controller.getMedicamentosRecetados);
}