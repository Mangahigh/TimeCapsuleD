/**
 * A shortcut to a for loop for repeating a task a few times
 * @param {number} times
 * @param {function} callback
 */
module.exports = (times, callback) => {
  for (let i = 0; i < times; ++i) {
    callback(i);
  }
};
