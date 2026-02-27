import ManualUpload from './components/ManualUpload'
import ScheduleManager from './components/ScheduleManager'
import QuarterRevenueUpload from './components/QuarterRevenueUpload'
import TDCCUpload from './components/TDCCUpload'
import CTEENewsUpload from './components/CTEENewsUpload'

function App() {
  return (
    <div className="app">
      <header className="header">
        <h1>台股資料管理介面</h1>
        <p className="subtitle">Tw Stock DB Operating</p>
      </header>
      <main className="main">
        <ManualUpload />
        <div className="side-panels">
          <ScheduleManager />
          <QuarterRevenueUpload />
          <TDCCUpload />
          <CTEENewsUpload />
        </div>
      </main>
    </div>
  )
}

export default App
