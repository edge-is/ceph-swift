var colors = require('colors');

colors.setTheme({
  input: 'grey',
  notice: 'cyan',
  prompt: 'grey',
  info: 'green',
  data: 'grey',
  help: 'cyan',
  warn: 'yellow',
  debug: 'grey',
  error: 'red'
});

var _log = {
  error : function (key){
    var self = this;
    return self._logger('error', key);
  },
  warn : function (key){
    var self = this;
    return self._logger('warn', key);
  },
  notice : function (key){
    var self = this;
    return self._logger('notice', key);
  },
  debug : function (key){
    var self = this;
    return self._logger('debug', key);
  },
  info : function (key){
    var self = this;
    return self._logger('info', key);
  },
  _logger : function (color, functionNane){
    return function (){
      var string = '';
      for ( var key in arguments){
        var argument = arguments[key];
        if(typeof argument === 'object'){
          string += "\n" + JSON.stringify(argument, null, 2);
        }else{
          string +=argument +' ';
        }

      }
      console.log(functionNane, colors[color](string));
    }

  }
};

module.exports = _log;
