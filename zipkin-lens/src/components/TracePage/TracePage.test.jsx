/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import React from 'react';
import { Provider } from 'react-redux';
import configureMockStore from 'redux-mock-store';
import thunk from 'redux-thunk';

import { TracePageImpl } from './TracePage';
import { loadTrace } from '../../slices/tracesSlice';
import { setAlert } from '../App/slice';

vi.mock('../../slices/tracesSlice', () => ({
  loadTrace: vi.fn((traceId) => ({ type: 'traces/load', payload: traceId })),
}));

vi.mock('../App/slice', () => ({
  setAlert: vi.fn((payload) => ({ type: 'app/setAlert', payload })),
}));

vi.mock('./TracePageContent', () => ({
  // eslint-disable-next-line react/prop-types
  TracePageContent: ({ trace, rawTrace }) => (
    <div
      data-testid="trace-page-content"
      data-trace={JSON.stringify(trace)}
      data-raw-trace={JSON.stringify(rawTrace)}
    />
  ),
}));

const traceId = 'abc123';

const mockStore = configureMockStore([thunk]);

// Builds the redux state shape that TracePageImpl's selector reads.
const buildStore = (traceState) =>
  mockStore({ traces: { traces: { [traceId]: traceState } } });

const match = { params: { traceId } };

const renderTracePage = (store) =>
  render(
    <Provider store={store}>
      <TracePageImpl match={match} />
    </Provider>,
  );

describe('<TracePageImpl />', () => {
  beforeEach(() => {
    loadTrace.mockClear();
    setAlert.mockClear();
  });
  afterEach(cleanup);

  it('should dispatch loadTrace with the traceId from the route on mount', () => {
    renderTracePage(buildStore({ isLoading: true }));

    expect(loadTrace).toHaveBeenCalledTimes(1);
    expect(loadTrace).toHaveBeenCalledWith(traceId);
  });

  it('should render the loading indicator while the trace is loading', () => {
    renderTracePage(buildStore({ isLoading: true }));

    expect(screen.getByTestId('loading-indicator')).toBeTruthy();
    expect(screen.queryByTestId('trace-page-content')).toBeNull();
  });

  it('should render nothing when the trace is missing and not loading', () => {
    const { container } = renderTracePage(buildStore({ isLoading: false }));

    expect(screen.queryByTestId('loading-indicator')).toBeNull();
    expect(screen.queryByTestId('trace-page-content')).toBeNull();
    expect(container.firstChild).toBeNull();
  });

  it('should render TracePageContent with the trace and rawTrace once loaded', () => {
    const adjustedTrace = { traceId, spans: [{ spanId: 's1' }] };
    const rawTrace = [{ traceId, id: 's1' }];
    renderTracePage(buildStore({ isLoading: false, adjustedTrace, rawTrace }));

    const content = screen.getByTestId('trace-page-content');
    expect(content).toBeTruthy();
    expect(JSON.parse(content.getAttribute('data-trace'))).toEqual(
      adjustedTrace,
    );
    expect(JSON.parse(content.getAttribute('data-raw-trace'))).toEqual(
      rawTrace,
    );
    expect(setAlert).not.toHaveBeenCalled();
  });

  it('should not render content when the trace was adjusted but the raw trace is missing', () => {
    const adjustedTrace = { traceId, spans: [] };
    const { container } = renderTracePage(
      buildStore({ isLoading: false, adjustedTrace }),
    );

    expect(screen.queryByTestId('trace-page-content')).toBeNull();
    expect(container.firstChild).toBeNull();
  });

  it('should not dispatch setAlert on the first render even when no trace exists', () => {
    renderTracePage(buildStore({ isLoading: false }));

    expect(setAlert).not.toHaveBeenCalled();
  });

  it('should dispatch an error alert when loading finishes without a trace', () => {
    const { rerender } = renderTracePage(buildStore({ isLoading: true }));

    // Loading finished but no trace was found: the alert effect should now fire.
    rerender(
      <Provider store={buildStore({ isLoading: false })}>
        <TracePageImpl match={match} />
      </Provider>,
    );

    expect(setAlert).toHaveBeenCalledWith({
      message: 'No trace found',
      severity: 'error',
    });
  });

  it('should include the error message in the alert when present', () => {
    const { rerender } = renderTracePage(buildStore({ isLoading: true }));

    rerender(
      <Provider
        store={buildStore({
          isLoading: false,
          error: { message: 'boom' },
        })}
      >
        <TracePageImpl match={match} />
      </Provider>,
    );

    expect(setAlert).toHaveBeenCalledWith({
      message: 'No trace found: boom',
      severity: 'error',
    });
  });
});
