// lib/litecoin-payment.js
// This file contains all core logic for interacting with the Litecoin network.

import bip39 from 'bip39';
import { BIP32Factory } from 'bip32'; // MODIFIED IMPORT
import * as ecc from 'tiny-secp256k1'; // NEW IMPORT
import * as bitcoin from 'bitcoinjs-lib';
import axios from 'axios';

// --- Initialize Bip32 with the Elliptic Curve Cryptography library ---
const bip32 = BIP32Factory(ecc); // NEW: Create the bip32 instance correctly

// --- Litecoin Network Configuration ---
const LITECOIN_NETWORK = {
    messagePrefix: '\x19Litecoin Signed Message:\n',
    bech32: 'ltc',
    bip32: {
        public: 0x019da462,
        private: 0x019d9cfe,
    },
    pubKeyHash: 0x30,
    scriptHash: 0x32,
    wif: 0xb0,
};

const LITECOIN_API_TOKEN = process.env.BLOCKCYPHER_API_TOKEN;
const LITECOIN_API_BASE_URL = `https://api.blockcypher.com/v1/ltc/main`;
const FEE_PER_BYTE_LITOSHIS = 2; // Fallback fee rate

/**
 * Derives a Litecoin P2WPKH (Bech32) address from a master seed and derivation path.
 * @param {string} seedPhrase - The BIP39 mnemonic for Litecoin.
 * @param {string} derivationPath - The BIP44 derivation path (e.g., m/44'/2'/0'/0/0).
 * @returns {{ address: string, keyPair: import('bip32').BIP32Interface }} The derived address and keypair object.
 */
export function deriveLitecoinAddress(seedPhrase, derivationPath) {
    const seed = bip39.mnemonicToSeedSync(seedPhrase);
    const root = bip32.fromSeed(seed, LITECOIN_NETWORK); // This will now work correctly
    const keyPair = root.derivePath(derivationPath);
    const { address } = bitcoin.payments.p2wpkh({ pubkey: keyPair.publicKey, network: LITECOIN_NETWORK });
    return { address, keyPair };
}

/**
 * Fetches the current recommended fee rate from the BlockCypher API.
 * @returns {Promise<number>} The recommended fee rate in litoshis per byte.
 */
export async function getLtcFeeRate() {
    const logPrefix = '[LTC_API FeeRate]';
    try {
        const url = `${LITECOIN_API_BASE_URL}`;
        const response = await axios.get(url, { params: { token: LITECOIN_API_TOKEN } });
        if (response.data && response.data.medium_fee_per_kb) {
            const feePerKb = response.data.medium_fee_per_kb;
            const feePerByte = Math.ceil(feePerKb / 1000);
            console.log(`${logPrefix} Fetched dynamic fee rate: ${feePerByte} litoshis/byte.`);
            return Math.max(2, feePerByte);
        } else {
            throw new Error("medium_fee_per_kb not found in API response.");
        }
    } catch (error) {
        console.error(`${logPrefix} Failed to fetch dynamic fee rate: ${error.message}. Using default.`);
        return FEE_PER_BYTE_LITOSHIS;
    }
}

/**
 * Fetches the balance and unspent transaction outputs (UTXOs) for a Litecoin address.
 * @param {string} address - The Litecoin address to query.
 * @returns {Promise<{balance: bigint, utxos: any[]}>}
 */
export async function getLtcBalanceAndUtxos(address) {
    const logPrefix = `[LTC_API Addr:${address.slice(0, 8)}]`;
    try {
        const url = `${LITECOIN_API_BASE_URL}/addrs/${address}?unspentOnly=true`;
        const response = await axios.get(url, { params: { token: LITECOIN_API_TOKEN } });
        const balance = BigInt(response.data.final_balance || 0);
        const utxos = response.data.txrefs || [];
        console.log(`${logPrefix} Balance: ${balance} Litoshis, UTXOs found: ${utxos.length}`);
        return { balance, utxos };
    } catch (error) {
        if (error.response && error.response.status === 404) {
            return { balance: 0n, utxos: [] };
        }
        console.error(`${logPrefix} Error fetching balance/UTXOs: ${error.message}`);
        throw error;
    }
}

/**
 * Builds, signs, and broadcasts a transaction to sweep all funds from a deposit address.
 * @param {import('bip32').BIP32Interface} keyPair - The keypair of the deposit address.
 * @param {string} targetAddress - The central hot wallet address to send funds to.
 * @returns {Promise<string>} The transaction hash (txid).
 */
export async function sweepLitecoinAddress(keyPair, targetAddress) {
    const { address } = bitcoin.payments.p2wpkh({ pubkey: keyPair.publicKey, network: LITECOIN_NETWORK });
    const logPrefix = `[LTC_Sweep Addr:${address.slice(0, 8)}]`;

    const { balance, utxos } = await getLtcBalanceAndUtxos(address);
    if (!utxos || utxos.length === 0) {
        throw new Error("No funds or UTXOs to sweep.");
    }
    
    const dynamicFeePerByte = await getLtcFeeRate();
    const psbt = new bitcoin.Psbt({ network: LITECOIN_NETWORK });
    let totalInput = 0n;

    // Use Promise.all to fetch transaction details concurrently
    const txDetailsPromises = utxos.map(utxo =>
        axios.get(`${LITECOIN_API_BASE_URL}/txs/${utxo.tx_hash}?includeHex=true`, { params: { token: LITECOIN_API_TOKEN } })
    );
    const txDetailsResponses = await Promise.all(txDetailsPromises);

    for (let i = 0; i < utxos.length; i++) {
        const utxo = utxos[i];
        const txDetailsResponse = txDetailsResponses[i];
        totalInput += BigInt(utxo.value);
        psbt.addInput({
            hash: utxo.tx_hash,
            index: utxo.tx_output_n,
            nonWitnessUtxo: Buffer.from(txDetailsResponse.data.hex, 'hex'),
        });
    }
    
    // Improved fee estimation by building a temporary transaction
    const tempPsbt = psbt.clone();
    tempPsbt.addOutput({ address: targetAddress, value: 5000 }); // Dummy value
    tempPsbt.signAllInputs(keyPair);
    tempPsbt.finalizeAllInputs();
    const vSize = tempPsbt.extractTransaction().virtualSize();
    const estimatedFee = BigInt(vSize * dynamicFeePerByte);

    const amountToSend = totalInput - estimatedFee;

    if (amountToSend <= 546) { // Dust limit check
        throw new Error(`Balance of ${totalInput} Litoshis is too low to cover the network fee of ${estimatedFee}.`);
    }
    
    psbt.addOutput({
        address: targetAddress,
        value: Number(amountToSend),
    });

    psbt.signAllInputs(keyPair);
    psbt.finalizeAllInputs();
    const txHex = psbt.extractTransaction().toHex();

    console.log(`${logPrefix} Broadcasting sweep transaction with dynamic fee...`);
    const broadcastUrl = `${LITECOIN_API_BASE_URL}/txs/push`;
    const response = await axios.post(broadcastUrl, { tx: txHex }, { params: { token: LITECOIN_API_TOKEN } });
    
    if (!response.data || !response.data.tx || !response.data.tx.hash) {
        throw new Error("API did not return a transaction hash after broadcasting.");
    }

    console.log(`${logPrefix} Sweep broadcast successful. TXID: ${response.data.tx.hash}`);
    return response.data.tx.hash;
}
