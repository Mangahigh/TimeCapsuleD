module.exports = {
  initialise:(config) => {
    return {
      getName: (name, queue)=>queue ? [config.redis.namespaces, queue, '__' + name].join('.') : [config.redis.namespaces, '__' + name].join('.')
    }
  }
};
