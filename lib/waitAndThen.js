/**
 * Allows for calling setTimeout with a promise style
 * e.g. waitAndThen(ms).then(() => console.log('All the things'));
 *
 * @param ms
 * @returns {Promise}
 */
module.exports = (ms) => {
  return new Promise((resolve) => {
    setTimeout(() => resolve(), ms);
  })
};
