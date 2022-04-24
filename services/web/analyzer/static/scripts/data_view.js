'use strict';

class DataViewManager {
  static defaultColumnWidth = 150;
  static defaultFontSize = 16;

  constructor() {
    this._entries = [];
  }

  init() {
    console.group('DataViewManager.init');
    if (!this.dataView) {
      console.info('User has not loaded a Dataset');
      this.hideToolWindow();
      return;
    }

    console.groupEnd();
    return Promise.all([]);
  }

  get dataView() {
    return app.dataView;
  }

  get entries() {
    return this._entries;
  }

  set entries(newEntries) {
    this._entries = newEntries;
  }

  showToolWindow() {
    const toolWindow = document.getElementById('toolWindow');
    show(toolWindow);
  }

  hideToolWindow() {
    const toolWindow = document.getElementById('toolWindow');
    hide(toolWindow);
  }

  async updateDataView() {
    console.trace();
    console.info('updateDataView', this.dataView);
    if (!this.dataView) {
      console.info('User has not loaded a Dataset');
      this.hideToolWindow();
      return;
    }

    this.showToolWindow();

    const payload = {
      data_view_id: this.dataView.id,
    };

    if (app.sortLabel) {
      console.info('setting payload.sort_label');
      payload.sort_label = app.sortLabel.name;
      payload.sort_dir = app.sortDir;
    }

    const url = buildRequest(
      services.rawEntriesAndTagsForDataView,
      payload,
    );

    console.info('updateDataView URL', url);

    try {
       const response = await fetch(url);
      if (response.status !== HTTP_OK) {
        console.log('Error processing request, status: ' + response.status);
        return;
      }

      const result = await response.json();
      if (!result.error) {
        this.entries = result['entries'];
        const tagsByKey = result['tags_by_key'];
        if (tagsByKey) {
          app.tagManager.updateMap(tagsByKey);
        }
        this.refreshDataView();
      } else {
        console.error(
          'error', result.error, 'retrieving data for DataView id =', this.dataView.id, ':', result.msg
        );
      }
    } catch(err) {
      console.log('Fetch Error:', err);
    }
  }

  refreshDataView() {
    const dataViewTable = document.getElementById('dataViewTable');

    console.info('DataViewManager.updateDataView');
    if (!this.dataView) {
      console.info('Cannot display data, dataView is', this.dataView);
    }
    const entries = this.entries;
    const labels = app.dataView.activeLabels;
    emptyElement(dataViewTable);

    if ((isIterable(labels) === false) || (objHasEntries(entries) === false)) {
      return;
    }

    const columnGroup = createColumnGroup({});
    let tableWidth = 0;
    for (const label of labels) {
      const columnWidth = (label.width || DataViewManager.defaultColumnWidth);

      columnGroup.appendChild(createColumn({
        id: 'col__' + label.