/* Ultra RPi -- Boot: initializes all modules */
(function () {
  'use strict';
  const U = window.Ultra;
  if (!U) {
    console.error('ultra-core.js must load before app.js');
    return;
  }
  U.initRun();
  U.initTabs();
  U.initSidebar();
  U.initCharts();
  U.initPanels();
  U.initEngineering();
  U.initFirmware();
  U.initConfigRecipes();
})();
