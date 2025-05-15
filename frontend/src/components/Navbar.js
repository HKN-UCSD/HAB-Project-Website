import Link from 'next/link';
import Image from 'next/image';

export default function Navbar() {
    return (
        <nav className="flex items-center justify-between p-4 text-white">
            <div className="text-lg font-bold">
                <Link href="https://hkn.ucsd.edu/">
                    <Image
                        src="/hknlogo3.svg"
                        className="ml-8"
                        alt="HKN Logo"
                        width={100}
                        height={40}
                    />
                </Link>
            </div>
        </nav>
    );
}
