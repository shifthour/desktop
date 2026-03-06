export default function Footer() {
  return (
    <footer className="bg-black text-white py-8">
      <div className="container mx-auto px-4">
        <div className="text-center space-y-4">
          {/* RERA Information */}
          <div className="space-y-2">
            <p className="text-sm">
              <span className="font-medium">Agent Rera No :</span> PRM/KA/RERA/1251/446/AG/250617/005841
            </p>
            <p className="text-sm">
              <span className="font-medium">Project RERA No:</span> PRM/KA/RERA/1250/304/PR/290425/007702,
              PRM/KA/RERA/1250/304/PR/050725/007898
            </p>
          </div>

          {/* Disclaimer */}
          <div className="max-w-4xl mx-auto">
            <p className="text-sm text-gray-300 leading-relaxed">
              <span className="font-medium text-white">Disclaimer -</span> The content provided on this website is for
              informational purposes only and does not constitute an offer to avail any service. The prices mentioned
              are subject to change without prior notice, and the availability of properties mentioned is not
              guaranteed.
            </p>
          </div>
        </div>
      </div>
    </footer>
  )
}
