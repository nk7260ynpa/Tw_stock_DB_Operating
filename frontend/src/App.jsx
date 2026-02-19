import ManualUpload from './components/ManualUpload'
import ScheduleManager from './components/ScheduleManager'

function App() {
  return (
    <div className="app">
      <header className="header">
        <h1>台股資料管理介面</h1>
        <p className="subtitle">Tw Stock DB Operating</p>
      </header>
      <main className="main">
        <ManualUpload />
        <ScheduleManager />
      </main>
    </div>
  )
}

export default App
