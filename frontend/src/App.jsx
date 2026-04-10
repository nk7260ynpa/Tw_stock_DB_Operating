import ManualUpload from './components/ManualUpload'
import ScheduleManager from './components/ScheduleManager'
import QuarterRevenueUpload from './components/QuarterRevenueUpload'
import TDCCUpload from './components/TDCCUpload'
import CTEENewsUpload from './components/CTEENewsUpload'
import CNYESNewsUpload from './components/CNYESNewsUpload'
import PTTNewsUpload from './components/PTTNewsUpload'
import MoneyUDNNewsUpload from './components/MoneyUDNNewsUpload'
import CompanyInfoUpload from './components/CompanyInfoUpload'
import RetryQueue from './components/RetryQueue'
import YTTranscriptUpload from './components/YTTranscriptUpload'
import OilPriceUpload from './components/OilPriceUpload'
import IndicesPriceUpload from './components/IndicesPriceUpload'
import YTSummaryGenerate from './components/YTSummaryGenerate'
import NewsSummaryGenerate from './components/NewsSummaryGenerate'

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
          <RetryQueue />
          <QuarterRevenueUpload />
          <TDCCUpload />
          <CompanyInfoUpload />
          <CTEENewsUpload />
          <CNYESNewsUpload />
          <PTTNewsUpload />
          <MoneyUDNNewsUpload />
          <YTTranscriptUpload />
          <OilPriceUpload />
          <IndicesPriceUpload />
          <YTSummaryGenerate />
          <NewsSummaryGenerate />
        </div>
      </main>
    </div>
  )
}

export default App
